package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/prometheus/tsdb/index"
)

func main() {
	var cfg Config

	flag.StringVar(&cfg.BlockPath, "block", "", "Block path in format s3://bucket/tenant/block-id")
	flag.StringVar(&cfg.MetricName, "metric-name", "", "Metric name to filter (optional)")
	flag.StringVar(&cfg.LabelKey, "label-key", "", "Label key to filter (optional)")
	flag.StringVar(&cfg.LabelValue, "label-value", "", "Label value to filter (optional)")
	flag.StringVar(&cfg.AWSRegion, "aws-region", "us-east-1", "AWS region")
	flag.StringVar(&cfg.AWSProfile, "aws-profile", "", "AWS profile name")
	flag.BoolVar(&cfg.Debug, "debug", false, "Enable debug output")
	flag.BoolVar(&cfg.CheckRegion, "check-region", false, "Only check and display the bucket's region, don't process")
	flag.BoolVar(&cfg.ForceIndexParallel, "force-index-parallel", false, "Force parallel download for index file")
	flag.IntVar(&cfg.ChunkWorkers, "chunk-workers", 20, "Number of parallel workers for chunk processing (default: 20)")
	flag.IntVar(&cfg.ChunkFileWorkers, "chunk-file-workers", 4, "Number of parallel workers processing chunk files (default: 4)")
	flag.IntVar(&cfg.ChunkTimeout, "chunk-timeout", 0, "Timeout per chunk in seconds (default: auto-calculated based on chunk size)")
	flag.StringVar(&cfg.WorkingDir, "working-dir", ".", "Working directory for caching downloaded files (default: current directory)")
	flag.Int64Var(&cfg.StartTime, "start-time", 0, "Start time (Unix timestamp in milliseconds, optional)")
	flag.Int64Var(&cfg.EndTime, "end-time", 0, "End time (Unix timestamp in milliseconds, optional)")
	flag.StringVar(&cfg.OutputFormat, "output", "csv", "Output format: csv, json, or prometheus")
	flag.StringVar(&cfg.OutputFilename, "ouput-filename", "", "Output filename (default random 4 digits)")
	flag.StringVar(&cfg.OutputLabels, "output-labels", "", "Comma separated list of labels to output as columns (CSV only)")
	flag.Float64Var(&cfg.SwitchThreshold, "switch-threshold", 0.2, "Switch to full download when this fraction of index file is requested (0.1-0.9, default: 0.2)")
	flag.BoolVar(&cfg.DumpChunkTable, "dump-chunk-table", false, "Dump chunk table (chunk file, offset, size) as CSV instead of time series data")
	flag.Parse()

	if cfg.BlockPath == "" {
		log.Fatal("Block path is required (format: s3://bucket/tenant/block-id)")
	}

	if err := dumpSeries(cfg); err != nil {
		log.Fatal(err)
	}
}

func dumpSeries(cfg Config) error {
	bucket, tenant, blockID, err := parseBlockPath(cfg.BlockPath)
	if err != nil {
		return fmt.Errorf("invalid block path: %w", err)
	}

	if cfg.Debug {
		fmt.Fprintf(os.Stderr, "Parsed block path:\n")
		fmt.Fprintf(os.Stderr, "  Bucket: %s\n", bucket)
		fmt.Fprintf(os.Stderr, "  Tenant: %s\n", tenant)
		fmt.Fprintf(os.Stderr, "  Block ID: %s\n", blockID)
	}

	var configOpts []func(*config.LoadOptions) error
	configOpts = append(configOpts, config.WithRegion(cfg.AWSRegion))
	if cfg.AWSProfile != "" {
		configOpts = append(configOpts, config.WithSharedConfigProfile(cfg.AWSProfile))
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(), configOpts...)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	if cfg.CheckRegion {
		fmt.Fprintf(os.Stderr, "Checking bucket region...\n")
		ctx := context.Background()
		bucketRegion, err := getBucketRegion(s3Client, bucket, ctx)
		if err != nil {
			return fmt.Errorf("failed to get bucket region: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Bucket '%s' is in region: %s\n", bucket, bucketRegion)
		fmt.Fprintf(os.Stderr, "You should use: -aws-region %s\n", bucketRegion)
		return nil
	}

	indexKey := path.Join(tenant, blockID, "index")
	indexCacheDir := ""
	if cfg.WorkingDir != "" {
		indexCacheDir = cfg.WorkingDir
	}

	indexReader, err := NewOptimizedS3ReaderWithCache(s3Client, bucket, indexKey, cfg.Debug, indexCacheDir, "index")
	if err != nil {
		if strings.Contains(err.Error(), "301") {
			fmt.Fprintf(os.Stderr, "Got 301 redirect - attempting region detection...\n")
			ctx := context.Background()
			bucketRegion, regionErr := getBucketRegion(s3Client, bucket, ctx)
			if regionErr != nil {
				return fmt.Errorf("failed to get bucket region: %w", regionErr)
			}

			fmt.Fprintf(os.Stderr, "Recreating client for region: %s\n", bucketRegion)
			newConfigOpts := []func(*config.LoadOptions) error{config.WithRegion(bucketRegion)}
			if cfg.AWSProfile != "" {
				newConfigOpts = append(newConfigOpts, config.WithSharedConfigProfile(cfg.AWSProfile))
			}

			newAwsCfg, err := config.LoadDefaultConfig(context.Background(), newConfigOpts...)
			if err != nil {
				return fmt.Errorf("failed to load AWS config for correct region: %w", err)
			}

			s3Client = s3.NewFromConfig(newAwsCfg, func(o *s3.Options) {
				o.UsePathStyle = true
			})

			indexReader, err = NewOptimizedS3ReaderWithCache(s3Client, bucket, indexKey, cfg.Debug, indexCacheDir, "index")
			if err != nil {
				return fmt.Errorf("failed to create index reader with correct region: %w", err)
			}
		} else {
			return fmt.Errorf("failed to create index reader: %w", err)
		}
	}

	indexReader.threshold = cfg.SwitchThreshold

	if cfg.WorkingDir != "" && !cfg.ForceIndexParallel {
		indexReader.threshold = 0.1
		if cfg.Debug {
			fmt.Fprintf(os.Stderr, "Working directory specified, lowered switch threshold to 10%% for better caching\n")
		}
	}

	if cfg.ForceIndexParallel {
		if cfg.Debug {
			fmt.Fprintf(os.Stderr, "Force parallel download requested for index file...\n")
		}
		if err := indexReader.downloadParallel(50 * 1024 * 1024); err != nil {
			return fmt.Errorf("failed to force download index file: %w", err)
		}
		if cfg.WorkingDir != "" {
			fmt.Fprintf(os.Stderr, "Saving force-downloaded index to cache...\n")
			indexReader.saveIndexToLocalCache()
		}
	}

	idx, err := index.NewReader(indexReader)
	if err != nil {
		return fmt.Errorf("failed to open index reader: %w", err)
	}
	defer idx.Close()

	defer func() {
		if cfg.WorkingDir != "" && indexReader.fileType == "index" && !indexReader.useFullData {
			if len(indexReader.indexRanges) > 0 {
				fmt.Fprintf(os.Stderr, "Processing completed, checking if we can reconstruct index from %d ranges...\n", len(indexReader.indexRanges))
				indexReader.reconstructAndCacheIndex()
			}
		}
	}()

	fmt.Fprintf(os.Stderr, "Reading chunk locations from index...\n")
	chunkInfos, err := getChunkReferences(*idx, cfg)
	if err != nil {
		return fmt.Errorf("failed to get chunk references: %w", err)
	}

	chunkFileStats := make(map[int]int)
	for _, chunkInfo := range chunkInfos {
		chunkFileStats[chunkInfo.ChunkFileNum]++
	}

	fmt.Fprintf(os.Stderr, "Found %d chunks distributed across chunk files:\n", len(chunkInfos))
	for fileNum, count := range chunkFileStats {
		fmt.Fprintf(os.Stderr, "  chunks/%06d: %d chunks\n", fileNum, count)
	}

	if cfg.DumpChunkTable {
		return outputChunkTable(chunkInfos, s3Client, bucket, tenant, blockID, cfg)
	}

	chunksByFile := make(map[int][]ChunkInfo)
	for _, chunkInfo := range chunkInfos {
		chunksByFile[chunkInfo.ChunkFileNum] = append(chunksByFile[chunkInfo.ChunkFileNum], chunkInfo)
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
				chunkFileName := fmt.Sprintf("%s/chunks/%06d", blockID, job.fileNum)
				chunksKey := path.Join(tenant, blockID, "chunks", fmt.Sprintf("%06d", job.fileNum))

				if cfg.Debug {
					fmt.Fprintf(os.Stderr, "Setting up chunks reader for s3://%s/%s (chunk file: %s, %d chunks)\n", bucket, chunksKey, chunkFileName, len(job.fileChunks))
				}

				chunkCacheDir := ""
				if cfg.WorkingDir != "" {
					chunkCacheDir = cfg.WorkingDir
				}

				chunksReader, err := NewOptimizedS3ReaderWithCache(s3Client, bucket, chunksKey, cfg.Debug, chunkCacheDir, "chunks")
				if err != nil {
					if strings.Contains(err.Error(), "301") {
						fmt.Fprintf(os.Stderr, "Got 301 redirect for chunk file - attempting region detection...\n")
						ctx := context.Background()
						bucketRegion, regionErr := getBucketRegion(s3Client, bucket, ctx)
						if regionErr != nil {
							errCh <- fmt.Errorf("failed to get bucket region: %w", regionErr)
							continue
						}

						fmt.Fprintf(os.Stderr, "Recreating client for region: %s\n", bucketRegion)
						newConfigOpts := []func(*config.LoadOptions) error{config.WithRegion(bucketRegion)}
						if cfg.AWSProfile != "" {
							newConfigOpts = append(newConfigOpts, config.WithSharedConfigProfile(cfg.AWSProfile))
						}

						newAwsCfg, err := config.LoadDefaultConfig(context.Background(), newConfigOpts...)
						if err != nil {
							errCh <- fmt.Errorf("failed to load AWS config for correct region: %w", err)
							continue
						}

						s3Client = s3.NewFromConfig(newAwsCfg, func(o *s3.Options) {
							o.UsePathStyle = true
						})

						chunksReader, err = NewOptimizedS3ReaderWithCache(s3Client, bucket, chunksKey, cfg.Debug, chunkCacheDir, "chunks")
						if err != nil {
							errCh <- fmt.Errorf("failed to create chunks reader with correct region for file %06d: %w", job.fileNum, err)
							continue
						}
					} else {
						errCh <- fmt.Errorf("failed to create chunks reader for file %06d: %w", job.fileNum, err)
						continue
					}
				}

				fmt.Fprintf(os.Stderr, "Reading time series data from %s (%d chunks)...\n", chunkFileName, len(job.fileChunks))
				points, err := readChunkData(chunksReader, job.fileChunks, cfg, chunkFileName)
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
