package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

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

                        // Parse chunk reference to extract file number, offset, and length.
                        // Chunk files on disk are numbered starting from 1, so add 1 to the
                        // extracted file number from the reference which starts at 0.
                        chunkRef := uint64(chk.Ref)
                        chunkFileNum := int((chunkRef >> 56) & 0xFF) + 1 // Top 8 bits
			chunkOffset := (chunkRef >> 24) & 0xFFFFFFFF // Next 32 bits
			chunkLength := uint32(chunkRef & 0xFFFFFF)   // Bottom 24 bits

			allChunkInfos = append(allChunkInfos, ChunkInfo{
				ChunkRef:     chk,
				SeriesLabel:  lbls,
				ChunkFileNum: chunkFileNum,
				ChunkOffset:  chunkOffset,
				ChunkLength:  chunkLength,
			})
		}
	}

	return allChunkInfos, postings.Err()
}

func outputChunkTable(chunkInfos []ChunkInfo) error {
	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()

	// Write header
	if err := writer.Write([]string{"series_labels", "chunk_file", "chunk_offset", "chunk_length", "min_time", "max_time"}); err != nil {
		return fmt.Errorf("failed to write chunk table header: %w", err)
	}

	// Write chunk data
	for _, chunkInfo := range chunkInfos {
		record := []string{
			chunkInfo.SeriesLabel.String(),
			fmt.Sprintf("%06d", chunkInfo.ChunkFileNum),
			strconv.FormatUint(chunkInfo.ChunkOffset, 10),
			strconv.FormatUint(uint64(chunkInfo.ChunkLength), 10),
			strconv.FormatInt(chunkInfo.ChunkRef.MinTime, 10),
			strconv.FormatInt(chunkInfo.ChunkRef.MaxTime, 10),
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write chunk table record: %w", err)
		}
	}

	fmt.Fprintf(os.Stderr, "Dumped chunk table with %d entries\n", len(chunkInfos))
	return nil
}

func readChunkData(chunksReader *OptimizedS3Reader, chunkInfos []ChunkInfo, cfg Config, chunkFileName string) ([]SeriesPoint, error) {
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

func processChunks(workerID int, jobs <-chan ChunkJob, results chan<- ChunkResult, chunksReader *OptimizedS3Reader, cfg Config, chunkFileName string) {
	for job := range jobs {
		points, err := processOneChunk(job, chunksReader, cfg, workerID, chunkFileName)
		results <- ChunkResult{
			Points:    points,
			Error:     err,
			Index:     job.Index,
			ChunkInfo: job.ChunkInfo,
		}
	}
}

func processOneChunk(job ChunkJob, chunksReader *OptimizedS3Reader, cfg Config, workerID int, chunkFileName string) ([]SeriesPoint, error) {
	// Read chunk data using the ChunkInfo offset and length
	chunkOffset := job.ChunkInfo.ChunkOffset
	chunkLength := job.ChunkInfo.ChunkLength

	if chunkLength == 0 {
		return nil, fmt.Errorf("chunk has zero length")
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
		sizeBasedTimeout := time.Duration(chunkLength/1024) * time.Second
		timeout = baseTimeout + sizeBasedTimeout

		if timeout < 30*time.Second {
			timeout = 30 * time.Second
		}
		if timeout > 5*time.Minute {
			timeout = 5 * time.Minute
		}
	}

	// Create a fresh context for each chunk read with calculated timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if cfg.Debug && job.Index%10000 == 0 {
		fmt.Fprintf(os.Stderr, "\nWorker %d: Processing chunk %d from %s (offset: %d, length: %d, timeout: %v)\n",
			workerID, job.Index+1, chunkFileName, chunkOffset, chunkLength, timeout)
	}

	chunkData := make([]byte, chunkLength)
	n, err := chunksReader.ReadAtWithContext(ctx, chunkData, int64(chunkOffset))
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read chunk data from %s (timeout: %v): %w", chunkFileName, timeout, err)
	}

	if n == 0 {
		return nil, fmt.Errorf("read 0 bytes from chunk in %s", chunkFileName)
	}

	// Decode chunk
	chunk, err := chunkenc.FromData(chunkenc.EncXOR, chunkData[:n])
	if err != nil {
		return nil, fmt.Errorf("failed to decode chunk from %s: %w", chunkFileName, err)
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
			Timestamp:    ts,
			Value:        val,
		})
	}

	if err := iter.Err(); err != nil {
		return points, fmt.Errorf("iterator error in %s: %w", chunkFileName, err)
	}

	if cfg.Debug && len(points) > 0 && job.Index%10000 == 0 {
		fmt.Fprintf(os.Stderr, "Worker %d: chunk %d from %s (series: %s) extracted %d points\n",
			workerID, job.Index+1, chunkFileName, labelsStr, len(points))
	}

	return points, nil
}
