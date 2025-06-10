package main

import (
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

// DataReader abstracts the operations required by chunk and index readers.
type DataReader interface {
	io.ReaderAt
	Len() int
	Range(start, end int) []byte
	Sub(start, end int) index.ByteSlice
	Size() int64
}

func getChunkReferences(idx index.Reader, cfg Config) ([]ChunkInfo, error) {
	var postings index.Postings
	var err error

	if cfg.MetricName != "" {
		postings, err = idx.Postings(labels.MetricName, cfg.MetricName)
		if err != nil {
			return nil, fmt.Errorf("failed to get postings for metric %s: %w", cfg.MetricName, err)
		}
	} else {
		postings, err = idx.Postings(index.AllPostingsKey())
		if err != nil {
			return nil, fmt.Errorf("failed to get all postings: %w", err)
		}
	}

	if cfg.LabelKey != "" && cfg.LabelValue != "" {
		labelPostings, err := idx.Postings(cfg.LabelKey, cfg.LabelValue)
		if err != nil {
			return nil, fmt.Errorf("failed to get postings for label %s=%s: %w", cfg.LabelKey, cfg.LabelValue, err)
		}
		postings = index.Intersect(postings, labelPostings)
	}

	var allChunkInfos []ChunkInfo
	var lbls labels.Labels
	var chks []chunks.Meta

	for postings.Next() {
		seriesID := postings.At()

		var builder labels.ScratchBuilder
		if err := idx.Series(seriesID, &builder, &chks); err != nil {
			return nil, fmt.Errorf("failed to get series %d: %w", seriesID, err)
		}

		lbls = builder.Labels()

		for _, chk := range chks {
			// Filter by time range if specified
			if cfg.StartTime > 0 && chk.MaxTime < cfg.StartTime {
				continue
			}
			if cfg.EndTime > 0 && chk.MinTime > cfg.EndTime {
				continue
			}

			// Parse chunk reference (BlockChunkRef) to extract file number and offset.
			// Chunk files on disk are numbered starting from 1, while the reference
			// stores them starting from 0. Length information is not part of the
			// reference and needs to be read from the chunk itself when processing.
			chunkRef := uint64(chk.Ref)
			chunkFileNum := int((chunkRef>>32)&0xFFFFFFFF) + 1 // Upper 4 bytes
			chunkOffset := chunkRef & 0xFFFFFFFF               // Lower 4 bytes

			allChunkInfos = append(allChunkInfos, ChunkInfo{
				ChunkRef:     chk,
				SeriesLabel:  lbls,
				ChunkFileNum: chunkFileNum,
				ChunkOffset:  chunkOffset,
				ChunkLength:  0,
			})
		}
	}

	return allChunkInfos, postings.Err()
}

func outputChunkTable(chunkInfos []ChunkInfo, s3Client *s3.Client, bucket, tenant, blockID string, cfg Config) error {
	outputPath, err := buildOutputPath(cfg, bucket, tenant, blockID, "csv")
	if err != nil {
		return fmt.Errorf("failed to build output path: %w", err)
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	if err := writer.Write([]string{"series_labels", "chunk_file", "chunk_offset", "chunk_length", "min_time", "max_time"}); err != nil {
		return fmt.Errorf("failed to write chunk table header: %w", err)
	}

	chunksByFile := make(map[int][]ChunkInfo)
	for _, ci := range chunkInfos {
		chunksByFile[ci.ChunkFileNum] = append(chunksByFile[ci.ChunkFileNum], ci)
	}

	for fileNum, fileChunks := range chunksByFile {
		chunksKey := path.Join(tenant, blockID, "chunks", fmt.Sprintf("%06d", fileNum))
		chunkCacheDir := ""
		if cfg.WorkingDir != "" {
			chunkCacheDir = cfg.WorkingDir
		}

		reader, err := NewOptimizedS3ReaderWithCache(s3Client, bucket, chunksKey, cfg.Debug, chunkCacheDir, "chunks", cfg.AWSProfile)
		if err != nil {
			return fmt.Errorf("failed to create chunks reader for file %06d: %w", fileNum, err)
		}

		for _, ci := range fileChunks {
			_, length, _, err := readRawChunk(reader, ci.ChunkOffset)
			if err != nil {
				return fmt.Errorf("failed to read chunk at offset %d in file %06d: %w", ci.ChunkOffset, fileNum, err)
			}
			record := []string{
				ci.SeriesLabel.String(),
				fmt.Sprintf("%06d", ci.ChunkFileNum),
				strconv.FormatUint(ci.ChunkOffset, 10),
				strconv.FormatUint(uint64(length), 10),
				strconv.FormatInt(ci.ChunkRef.MinTime, 10),
				strconv.FormatInt(ci.ChunkRef.MaxTime, 10),
			}
			if err := writer.Write(record); err != nil {
				return fmt.Errorf("failed to write chunk table record: %w", err)
			}
		}
	}

	fmt.Fprintf(os.Stderr, "Dumped chunk table with %d entries to %s\n", len(chunkInfos), outputPath)
	return nil
}

func readChunkData(chunksReader DataReader, chunkInfos []ChunkInfo, cfg Config, chunkFileName string) ([]SeriesPoint, error) {
	var allPoints []SeriesPoint
	startTime := time.Now()

	fmt.Fprintf(os.Stderr, "Processing %d chunks from %s using %d parallel workers...\n", len(chunkInfos), chunkFileName, cfg.ChunkWorkers)

	// Create job and result channels
	jobs := make(chan ChunkJob, len(chunkInfos))
	results := make(chan ChunkResult, len(chunkInfos))

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < cfg.ChunkWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			processChunks(workerID, jobs, results, chunksReader, cfg, chunkFileName)
		}(w)
	}

	// Send jobs to workers
	go func() {
		for i, chunkInfo := range chunkInfos {
			jobs <- ChunkJob{
				Index:     i,
				ChunkInfo: chunkInfo,
			}
		}
		close(jobs)
	}()

	// Progress tracking
	processed := 0
	lastProgressTime := time.Now()
	totalPoints := 0

	// Collect results
	go func() {
		wg.Wait()
		close(results)
	}()

	for result := range results {
		processed++

		if result.Error != nil {
			fmt.Fprintf(os.Stderr, "\nWarning: chunk %d/%d from %s (offset: %d, length: %d) failed: %v\n",
				result.Index+1, len(chunkInfos), chunkFileName, result.ChunkInfo.ChunkOffset, result.ChunkInfo.ChunkLength, result.Error)
		} else {
			allPoints = append(allPoints, result.Points...)
			totalPoints += len(result.Points)
		}

		// Show progress every 1000 chunks or every 10 seconds
		if processed%1000 == 0 || time.Since(lastProgressTime) > 10*time.Second {
			elapsed := time.Since(startTime)
			rate := float64(processed) / elapsed.Seconds()
			remaining := time.Duration(float64(len(chunkInfos)-processed)/rate) * time.Second
			progress := float64(processed) / float64(len(chunkInfos)) * 100

			fmt.Fprintf(os.Stderr, "\rChunk progress: %d/%d (%.1f%%) - Rate: %.0f chunks/sec - ETA: %v - Points: %d",
				processed, len(chunkInfos), progress, rate, remaining, totalPoints)
			lastProgressTime = time.Now()
		}
	}

	// Clear progress line and show final stats
	elapsed := time.Since(startTime)
	fmt.Fprintf(os.Stderr, "\rCompleted processing %d chunks from %s in %v using %d workers - Extracted %d data points                    \n",
		len(chunkInfos), chunkFileName, elapsed, cfg.ChunkWorkers, len(allPoints))

	return allPoints, nil
}

func processChunks(workerID int, jobs <-chan ChunkJob, results chan<- ChunkResult, chunksReader DataReader, cfg Config, chunkFileName string) {
	for job := range jobs {
		points, info, err := processOneChunk(job, chunksReader, cfg, workerID, chunkFileName)
		results <- ChunkResult{
			Points:    points,
			Error:     err,
			Index:     job.Index,
			ChunkInfo: info,
		}
	}
}

func processOneChunk(job ChunkJob, chunksReader DataReader, cfg Config, workerID int, chunkFileName string) ([]SeriesPoint, ChunkInfo, error) {
	info := job.ChunkInfo
	chunkOffset := info.ChunkOffset

	raw, chunkLen, enc, err := readRawChunk(chunksReader, chunkOffset)
	info.ChunkLength = chunkLen
	if err != nil {
		return nil, info, fmt.Errorf("failed to read chunk data from %s: %w", chunkFileName, err)
	}

	// Calculate adaptive timeout based on chunk size and configuration
	var timeout time.Duration
	if cfg.ChunkTimeout > 0 {
		// Use user-specified timeout
		timeout = time.Duration(cfg.ChunkTimeout) * time.Second
	} else {
		// Auto-calculate timeout based on chunk size
		// Base timeout: 30 seconds
		// Additional time: 1 second per 1KB of chunk data
		// Minimum: 30 seconds, Maximum: 5 minutes
		baseTimeout := 30 * time.Second
		sizeBasedTimeout := time.Duration(chunkLen/1024) * time.Second
		timeout = baseTimeout + sizeBasedTimeout

		if timeout < 30*time.Second {
			timeout = 30 * time.Second
		}
		if timeout > 5*time.Minute {
			timeout = 5 * time.Minute
		}
	}

	// Create a fresh context for each chunk read with calculated timeout

	if cfg.Debug && job.Index%10000 == 0 {
		fmt.Fprintf(os.Stderr, "\nWorker %d: Processing chunk %d from %s (offset: %d, length: %d, timeout: %v)\n",
			workerID, job.Index+1, chunkFileName, chunkOffset, chunkLen, timeout)
	}

	// Decode chunk
	chunk, err := chunkenc.FromData(chunkenc.Encoding(enc), raw)
	if err != nil {
		return nil, info, fmt.Errorf("failed to decode chunk from %s: %w", chunkFileName, err)
	}

	// Extract time series points
	iter := chunk.Iterator(nil)
	labelsStr := job.ChunkInfo.SeriesLabel.String()
	var points []SeriesPoint

	for iter.Next() == chunkenc.ValFloat {
		ts, val := iter.At()

		// Apply time range filter
		if cfg.StartTime > 0 && ts < cfg.StartTime {
			continue
		}
		if cfg.EndTime > 0 && ts > cfg.EndTime {
			continue
		}

		points = append(points, SeriesPoint{
			SeriesLabels: labelsStr,
			Labels:       job.ChunkInfo.SeriesLabel,
			Timestamp:    ts,
			Value:        val,
		})
	}

	if err := iter.Err(); err != nil {
		return points, info, fmt.Errorf("iterator error in %s: %w", chunkFileName, err)
	}

	if cfg.Debug && len(points) > 0 && job.Index%10000 == 0 {
		fmt.Fprintf(os.Stderr, "Worker %d: chunk %d from %s (series: %s) extracted %d points\n",
			workerID, job.Index+1, chunkFileName, labelsStr, len(points))
	}

	return points, info, nil
}

func readRawChunk(r DataReader, offset uint64) ([]byte, uint32, byte, error) {
	header := make([]byte, chunks.MaxChunkLengthFieldSize+1)
	if _, err := r.ReadAt(header, int64(offset)); err != nil && err != io.EOF {
		return nil, 0, 0, err
	}

	dataLen, n := binary.Uvarint(header[:chunks.MaxChunkLengthFieldSize])
	if n <= 0 {
		return nil, 0, 0, fmt.Errorf("reading chunk length failed with %d", n)
	}
	enc := header[n]
	total := n + 1 + int(dataLen) + crc32.Size

	buf := make([]byte, total)
	if _, err := r.ReadAt(buf, int64(offset)); err != nil && err != io.EOF {
		return nil, 0, 0, err
	}

	dataStart := n + 1
	dataEnd := dataStart + int(dataLen)
	sum := buf[dataEnd : dataEnd+crc32.Size]
	if err := verifyCRC32(buf[n:dataEnd], sum); err != nil {
		return nil, 0, 0, err
	}

	return buf[dataStart:dataEnd], uint32(total), enc, nil
}

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

func verifyCRC32(data, sum []byte) error {
	want := binary.BigEndian.Uint32(sum)
	got := crc32.Checksum(data, castagnoliTable)
	if want != got {
		return fmt.Errorf("checksum mismatch expected:%x actual:%x", want, got)
	}
	return nil
}
