package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
)

func dumpSeriesLocal(cfg Config, blockDir, bucket, tenant, blockID string) error {
	if cfg.Debug {
		fmt.Fprintf(os.Stderr, "Reading local block from pieces at %s\n", blockDir)
	}

	indexPath := filepath.Join(blockDir, "index")
	indexReader, err := NewLocalPieceReader(indexPath, "index", cfg.Debug)
	if err != nil {
		return fmt.Errorf("failed to create local index reader: %w", err)
	}

	idx, err := index.NewReader(indexReader)
	if err != nil {
		return fmt.Errorf("failed to open index reader: %w", err)
	}
	defer idx.Close()

	fmt.Fprintf(os.Stderr, "Reading chunk locations from index...\n")
	chunkInfos, err := getChunkReferences(*idx, cfg)
	if err != nil {
		return fmt.Errorf("failed to get chunk references: %w", err)
	}

	chunkFileStats := make(map[int]int)
	for _, ci := range chunkInfos {
		chunkFileStats[ci.ChunkFileNum]++
	}

	fmt.Fprintf(os.Stderr, "Found %d chunks distributed across chunk files:\n", len(chunkInfos))
	for fileNum, count := range chunkFileStats {
		fmt.Fprintf(os.Stderr, "  chunks/%06d: %d chunks\n", fileNum, count)
	}

	if cfg.DumpChunkTable {
		// Build and output chunk table using local pieces
		return outputChunkTableLocal(chunkInfos, blockDir, bucket, tenant, blockID, cfg)
	}

	chunksByFile := make(map[int][]ChunkInfo)
	for _, ci := range chunkInfos {
		chunksByFile[ci.ChunkFileNum] = append(chunksByFile[ci.ChunkFileNum], ci)
	}

	type chunkFileJob struct {
		fileNum    int
		fileChunks []ChunkInfo
	}

	fileJobs := make(chan chunkFileJob)
	results := make(chan []SeriesPoint)
	errCh := make(chan error, len(chunksByFile))

	var wg sync.WaitGroup
	for w := 0; w < cfg.ChunkFileWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range fileJobs {
				chunkDir := filepath.Join(blockDir, "chunks", fmt.Sprintf("%06d", job.fileNum))
				chunkFileName := fmt.Sprintf("%s/chunks/%06d", blockID, job.fileNum)

				reader, err := NewLocalPieceReader(chunkDir, "chunks", cfg.Debug)
				if err != nil {
					errCh <- fmt.Errorf("failed to create local chunks reader for file %06d: %w", job.fileNum, err)
					continue
				}

				fmt.Fprintf(os.Stderr, "Reading time series data from %s (%d chunks)...\n", chunkFileName, len(job.fileChunks))
				points, err := readChunkData(reader, job.fileChunks, cfg, chunkFileName)
				if err != nil {
					errCh <- fmt.Errorf("failed to read chunk data from %s: %w", chunkFileName, err)
					continue
				}

				results <- points
			}
		}()
	}

	go func() {
		for fileNum, fileChunks := range chunksByFile {
			fileJobs <- chunkFileJob{fileNum: fileNum, fileChunks: fileChunks}
		}
		close(fileJobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	var allPoints []SeriesPoint
	for pts := range results {
		allPoints = append(allPoints, pts...)
	}

	close(errCh)
	if len(errCh) > 0 {
		return <-errCh
	}

	fmt.Fprintf(os.Stderr, "Extracted %d data points from %d chunk files\n", len(allPoints), len(chunksByFile))

	return outputResults(allPoints, cfg, bucket, tenant, blockID)
}

func postingsFromConfig(idx tsdb.IndexReader, cfg Config) (index.Postings, error) {
	var (
		p   index.Postings
		err error
	)
	if cfg.MetricName != "" {
		p, err = idx.Postings(labels.MetricName, cfg.MetricName)
	} else {
		p, err = idx.Postings(index.AllPostingsKey())
	}
	if err != nil {
		return nil, err
	}
	if cfg.LabelKey != "" && cfg.LabelValue != "" {
		lp, err := idx.Postings(cfg.LabelKey, cfg.LabelValue)
		if err != nil {
			return nil, err
		}
		p = index.Intersect(p, lp)
	}
	return p, nil
}

func localBlockPath(cfg Config, bucket, tenant, blockID string) string {
	return filepath.Join(cfg.WorkingDir, bucket, tenant, blockID)
}

func outputChunkTableLocal(chunkInfos []ChunkInfo, blockDir, bucket, tenant, blockID string, cfg Config) error {
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

	if err := writer.Write([]string{"series_labels", "chunk_file", "chunk_offset", "chunk_length", "min_time", "max_time"}); err != nil {
		return fmt.Errorf("failed to write chunk table header: %w", err)
	}

	chunksByFile := make(map[int][]ChunkInfo)
	for _, ci := range chunkInfos {
		chunksByFile[ci.ChunkFileNum] = append(chunksByFile[ci.ChunkFileNum], ci)
	}

	for fileNum, fileChunks := range chunksByFile {
		chunkDir := filepath.Join(blockDir, "chunks", fmt.Sprintf("%06d", fileNum))
		reader, err := NewLocalPieceReader(chunkDir, "chunks", cfg.Debug)
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
