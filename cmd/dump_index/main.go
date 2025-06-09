package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type Config struct {
	BlockPath          string
	MetricName         string
	LabelKey           string
	LabelValue         string
	AWSRegion          string
	AWSProfile         string
	Debug              bool
	CheckRegion        bool
	ForceIndexParallel bool
	ChunkWorkers       int
	ChunkTimeout       int
	WorkingDir         string
	StartTime          int64
	EndTime            int64
	OutputFormat       string
	SwitchThreshold    float64
}

type SeriesPoint struct {
	SeriesLabels string
	Timestamp    int64
	Value        float64
}

type ChunkJob struct {
	Index       int
	ChunkRef    chunks.Meta
	SeriesLabel labels.Labels
}

type ChunkResult struct {
	Points   []SeriesPoint
	Error    error
	Index    int
	ChunkRef chunks.Meta
}

type OptimizedS3Reader struct {
	client         *s3.Client
	bucket         string
	key            string
	size           int64
	cache          map[string][]byte
	debug          bool
	data           []byte
	useFullData    bool
	mu             sync.RWMutex
	totalRequested int64
	requestCount   int
	threshold      float64
	localCacheDir  string
	fileType       string
	indexRanges    map[int64][]byte // For accumulating index ranges
}

func NewOptimizedS3Reader(client *s3.Client, bucket, key string, debug bool) (*OptimizedS3Reader, error) {
	return NewOptimizedS3ReaderWithCache(client, bucket, key, debug, "", "")
}

func NewOptimizedS3ReaderWithCache(client *s3.Client, bucket, key string, debug bool, cacheDir, fileType string) (*OptimizedS3Reader, error) {
	headResp, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object info: %w", err)
	}

	return &OptimizedS3Reader{
		client:        client,
		bucket:        bucket,
		key:           key,
		size:          *headResp.ContentLength,
		cache:         make(map[string][]byte),
		debug:         debug,
		useFullData:   false,
		threshold:     0.3,
		localCacheDir: cacheDir,
		fileType:      fileType,
		indexRanges:   make(map[int64][]byte),
	}, nil
}

func (r *OptimizedS3Reader) ReadAt(p []byte, off int64) (n int, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	return r.ReadAtWithContext(ctx, p, off)
}

func (r *OptimizedS3Reader) ReadAtWithContext(ctx context.Context, p []byte, off int64) (n int, err error) {
	if off >= r.size {
		return 0, io.EOF
	}

	end := off + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}

	if r.useFullData {
		requestedLength := end - off + 1
		copy(p, r.data[off:off+requestedLength])
		return int(requestedLength), nil
	}

	// Check local cache first if caching is enabled
	if r.localCacheDir != "" {
		if r.fileType == "index" {
			// For index files, check if we have the complete file cached
			cachedData, found := r.readFromLocalCache(0, r.size)
			if found {
				r.data = cachedData
				r.useFullData = true
				if r.debug {
					fmt.Fprintf(os.Stderr, "Loaded complete index from cache\n")
				}
				requestedLength := end - off + 1
				copy(p, r.data[off:off+requestedLength])
				return int(requestedLength), nil
			}
		} else if r.fileType == "chunks" {
			// For chunk files, check specific range cache
			cachedData, found := r.readFromLocalCache(off, end-off+1)
			if found {
				copy(p, cachedData)
				return len(cachedData), nil
			}
		}
	}

	// Check if we should switch to full download for index files
	r.mu.Lock()
	requestedDataRatio := float64(r.totalRequested) / float64(r.size)
	shouldSwitchToFull := requestedDataRatio > r.threshold || r.requestCount > 15
	r.mu.Unlock()

	if shouldSwitchToFull && !r.useFullData && r.fileType == "index" {
		if r.debug {
			fmt.Fprintf(os.Stderr, "Switching to full download: %.1f%% of file requested in %d requests\n", 
				requestedDataRatio*100, r.requestCount)
		}
		
		if err := r.downloadParallel(50 * 1024 * 1024); err != nil {
			if r.debug {
				fmt.Fprintf(os.Stderr, "Failed to download full file, continuing with range requests: %v\n", err)
			}
		} else {
			if r.localCacheDir != "" && r.fileType == "index" {
				r.saveIndexToLocalCache()
			}
			requestedLength := end - off + 1
			copy(p, r.data[off:off+requestedLength])
			return int(requestedLength), nil
		}
	}

	// Continue with range requests
	r.mu.RLock()
	cacheKey := fmt.Sprintf("%d-%d", off, end)
	if data, exists := r.cache[cacheKey]; exists {
		r.mu.RUnlock()
		copy(p, data)
		return len(data), nil
	}
	r.mu.RUnlock()

	// Use larger range optimization - 256KB instead of 64KB
	optimizedStart := (off / 262144) * 262144
	optimizedEnd := ((end / 262144) + 1) * 262144 - 1
	if optimizedEnd >= r.size {
		optimizedEnd = r.size - 1
	}

	rangeHeader := fmt.Sprintf("bytes=%d-%d", optimizedStart, optimizedEnd)
	
	resp, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(r.key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to read range %s: %w", rangeHeader, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read response body: %w", err)
	}

	// Save to local cache if enabled
	if r.localCacheDir != "" {
		if r.fileType == "chunks" {
			r.saveChunkToLocalCache(optimizedStart, data)
		} else if r.fileType == "index" {
			// For index files, accumulate range data and save when we have enough
			r.saveIndexRangeToLocalCache(optimizedStart, data)
		}
	}

	// Update statistics and cache
	r.mu.Lock()
	optimizedCacheKey := fmt.Sprintf("%d-%d", optimizedStart, optimizedEnd)
	r.cache[optimizedCacheKey] = data
	r.totalRequested += int64(len(data))
	r.requestCount++
	r.mu.Unlock()

	// Extract the requested portion
	requestedOffset := off - optimizedStart
	requestedLength := end - off + 1
	
	if requestedOffset+requestedLength > int64(len(data)) {
		requestedLength = int64(len(data)) - requestedOffset
	}

	copy(p, data[requestedOffset:requestedOffset+requestedLength])
	return int(requestedLength), nil
}

func (r *OptimizedS3Reader) downloadParallel(chunkSize int64) error {
	numChunks := (r.size + chunkSize - 1) / chunkSize
	
	if r.debug {
		fmt.Fprintf(os.Stderr, "Downloading entire file in %d parallel chunks of %d MB each...\n", 
			numChunks, chunkSize/(1024*1024))
	}

	r.data = make([]byte, r.size)
	startTime := time.Now()
	
	var totalWritten int64
	var mu sync.Mutex
	
	progressTicker := time.NewTicker(1 * time.Second)
	defer progressTicker.Stop()
	
	done := make(chan bool)
	go func() {
		defer close(done)
		for {
			select {
			case <-progressTicker.C:
				mu.Lock()
				written := totalWritten
				mu.Unlock()
				if written > 0 {
					progress := float64(written) / float64(r.size) * 100
					elapsed := time.Since(startTime)
					rate := float64(written) / (1024 * 1024) / elapsed.Seconds()
					fmt.Fprintf(os.Stderr, "\rDownload progress: %.1f%% (%d/%d bytes) - %.2f MB/s", 
						progress, written, r.size, rate)
				}
			case <-done:
				return
			}
		}
	}()

	if chunkSize < 50*1024*1024 {
		chunkSize = 50 * 1024 * 1024
		numChunks = (r.size + chunkSize - 1) / chunkSize
	}

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 20)
	errChan := make(chan error, numChunks)

	for i := int64(0); i < numChunks; i++ {
		wg.Add(1)
		go func(chunkNum int64) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			start := chunkNum * chunkSize
			end := start + chunkSize - 1
			if end >= r.size {
				end = r.size - 1
			}

			maxRetries := 3
			var chunkData []byte
			var err error
			
			for retry := 0; retry < maxRetries; retry++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				
				rangeHeader := fmt.Sprintf("bytes=%d-%d", start, end)
				resp, reqErr := r.client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(r.bucket),
					Key:    aws.String(r.key),
					Range:  aws.String(rangeHeader),
				})
				
				if reqErr != nil {
					cancel()
					err = reqErr
					if retry < maxRetries-1 {
						time.Sleep(time.Duration(retry+1) * time.Second)
						continue
					}
					errChan <- fmt.Errorf("failed to download chunk %d after %d retries: %w", chunkNum, maxRetries, reqErr)
					return
				}

				chunkData, err = io.ReadAll(resp.Body)
				resp.Body.Close()
				cancel()
				
				if err == nil {
					break
				}
				
				if retry < maxRetries-1 {
					time.Sleep(time.Duration(retry+1) * time.Second)
				}
			}
			
			if err != nil {
				errChan <- fmt.Errorf("failed to read chunk %d after %d retries: %w", chunkNum, maxRetries, err)
				return
			}

			copy(r.data[start:start+int64(len(chunkData))], chunkData)

			mu.Lock()
			totalWritten += int64(len(chunkData))
			mu.Unlock()

		}(i)
	}

	wg.Wait()
	done <- true
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	r.useFullData = true
	
	elapsed := time.Since(startTime)
	rate := float64(r.size) / (1024 * 1024) / elapsed.Seconds()
	fmt.Fprintf(os.Stderr, "\rFull download completed: %d bytes in %v (%.2f MB/s)          \n", 
		r.size, elapsed, rate)
	
	return nil
}

func (r *OptimizedS3Reader) Len() int {
	return int(r.size)
}

func (r *OptimizedS3Reader) Range(start, end int) []byte {
	if start < 0 || end > int(r.size) || start >= end {
		return nil
	}
	
	if r.useFullData {
		return r.data[start:end]
	}
	
	data := make([]byte, end-start)
	n, err := r.ReadAt(data, int64(start))
	if err != nil && err != io.EOF {
		return nil
	}
	
	return data[:n]
}

func (r *OptimizedS3Reader) Sub(start, end int) index.ByteSlice {
	if start < 0 || end > int(r.size) || start >= end {
		return nil
	}
	
	data := r.Range(start, end)
	return &simpleByteSlice{data: data}
}

func (r *OptimizedS3Reader) Size() int64 {
	return r.size
}

func (r *OptimizedS3Reader) GetStats() (int, int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	if r.useFullData {
		return 1, len(r.data)
	}
	
	totalRanges := len(r.cache)
	totalBytes := int(r.totalRequested)
	return totalRanges, totalBytes
}

func (r *OptimizedS3Reader) readFromLocalCache(offset int64, length int64) ([]byte, bool) {
	if r.fileType == "index" {
		indexPath := filepath.Join(r.localCacheDir, r.bucket, strings.ReplaceAll(r.key, "/", string(filepath.Separator)), "index")
		if data, err := os.ReadFile(indexPath); err == nil {
			if int64(len(data)) >= offset+length {
				return data[offset : offset+length], true
			}
		}
	} else if r.fileType == "chunks" {
		chunkCacheDir := filepath.Join(r.localCacheDir, r.bucket, strings.ReplaceAll(r.key, "/", string(filepath.Separator)))
		chunkFile := filepath.Join(chunkCacheDir, fmt.Sprintf("%d_%d.bin", offset, length))
		if data, err := os.ReadFile(chunkFile); err == nil {
			if r.debug {
				fmt.Fprintf(os.Stderr, "Cache hit: %s\n", chunkFile)
			}
			return data, true
		}
	}
	return nil, false
}

func (r *OptimizedS3Reader) saveIndexToLocalCache() {
	if r.data == nil || r.localCacheDir == "" {
		return
	}
	
	indexPath := filepath.Join(r.localCacheDir, r.bucket, strings.ReplaceAll(r.key, "/", string(filepath.Separator)), "index")
	if err := os.MkdirAll(filepath.Dir(indexPath), 0755); err != nil {
		if r.debug {
			fmt.Fprintf(os.Stderr, "Failed to create cache directory: %v\n", err)
		}
		return
	}
	
	if err := os.WriteFile(indexPath, r.data, 0644); err != nil {
		if r.debug {
			fmt.Fprintf(os.Stderr, "Failed to cache index file: %v\n", err)
		}
	} else if r.debug {
		fmt.Fprintf(os.Stderr, "Cached index file to: %s\n", indexPath)
	}
}

func (r *OptimizedS3Reader) saveChunkToLocalCache(offset int64, data []byte) {
	if r.localCacheDir == "" {
		return
	}
	
	chunkCacheDir := filepath.Join(r.localCacheDir, r.bucket, strings.ReplaceAll(r.key, "/", string(filepath.Separator)))
	if err := os.MkdirAll(chunkCacheDir, 0755); err != nil {
		if r.debug {
			fmt.Fprintf(os.Stderr, "Failed to create chunk cache directory: %v\n", err)
		}
		return
	}
	
	chunkFile := filepath.Join(chunkCacheDir, fmt.Sprintf("%d_%d.bin", offset, len(data)))
	if err := os.WriteFile(chunkFile, data, 0644); err != nil {
		if r.debug {
			fmt.Fprintf(os.Stderr, "Failed to cache chunk: %v\n", err)
		}
	} else if r.debug {
		fmt.Fprintf(os.Stderr, "Cached chunk to: %s\n", chunkFile)
	}
}

func (r *OptimizedS3Reader) saveIndexRangeToLocalCache(offset int64, data []byte) {
	if r.localCacheDir == "" {
		return
	}

	// Accumulate index ranges in memory
	r.indexRanges[offset] = data
	
	// Check if we should try to reconstruct the full index
	// This is a simple heuristic - if we have enough data, try to reconstruct
	totalCachedBytes := int64(0)
	for _, rangeData := range r.indexRanges {
		totalCachedBytes += int64(len(rangeData))
	}
	
	// If we have >= 90% of the file in ranges, reconstruct and cache the full index
	if float64(totalCachedBytes) >= float64(r.size)*0.9 {
		r.reconstructAndCacheIndex()
	}
}

func (r *OptimizedS3Reader) reconstructAndCacheIndex() {
	if r.debug {
		fmt.Fprintf(os.Stderr, "Attempting to reconstruct full index from %d cached ranges...\n", len(r.indexRanges))
	}

	// Create a map of all byte positions we have
	reconstructed := make([]byte, r.size)
	covered := make([]bool, r.size)
	
	for offset, data := range r.indexRanges {
		if offset+int64(len(data)) <= r.size {
			copy(reconstructed[offset:offset+int64(len(data))], data)
			for i := offset; i < offset+int64(len(data)); i++ {
				covered[i] = true
			}
		}
	}
	
	// Check how much we have covered
	coveredBytes := int64(0)
	for _, isCovered := range covered {
		if isCovered {
			coveredBytes++
		}
	}
	
	coverage := float64(coveredBytes) / float64(r.size)
	if r.debug {
		fmt.Fprintf(os.Stderr, "Index reconstruction coverage: %.1f%% (%d/%d bytes)\n", 
			coverage*100, coveredBytes, r.size)
	}
	
	// If we have good coverage (>=95%), save the index
	if coverage >= 0.95 {
		indexPath := filepath.Join(r.localCacheDir, r.bucket, strings.ReplaceAll(r.key, "/", string(filepath.Separator)), "index")
		if err := os.MkdirAll(filepath.Dir(indexPath), 0755); err != nil {
			if r.debug {
				fmt.Fprintf(os.Stderr, "Failed to create index cache directory: %v\n", err)
			}
			return
		}
		
		if err := os.WriteFile(indexPath, reconstructed, 0644); err != nil {
			if r.debug {
				fmt.Fprintf(os.Stderr, "Failed to cache reconstructed index: %v\n", err)
			}
		} else if r.debug {
			fmt.Fprintf(os.Stderr, "Successfully cached reconstructed index to: %s\n", indexPath)
		}
		
		// Also update our in-memory data for immediate use
		r.data = reconstructed
		r.useFullData = true
		
		// Clear the ranges map to save memory
		r.indexRanges = make(map[int64][]byte)
	}
}

type simpleByteSlice struct {
	data []byte
}

func (s *simpleByteSlice) Len() int {
	return len(s.data)
}

func (s *simpleByteSlice) Range(start, end int) []byte {
	if start < 0 || end > len(s.data) || start >= end {
		return nil
	}
	return s.data[start:end]
}

func (s *simpleByteSlice) Sub(start, end int) index.ByteSlice {
	if start < 0 || end > len(s.data) || start >= end {
		return nil
	}
	return &simpleByteSlice{data: s.data[start:end]}
}

func getBucketRegion(s3Client *s3.Client, bucket string, ctx context.Context) (string, error) {
	locationResp, err := s3Client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get bucket location: %w", err)
	}
	
	if locationResp.LocationConstraint == "" {
		return "us-east-1", nil
	}
	
	return string(locationResp.LocationConstraint), nil
}

func parseBlockPath(blockPath string) (bucket, tenant, blockID string, err error) {
	if !strings.HasPrefix(blockPath, "s3://") {
		return "", "", "", fmt.Errorf("block path must start with s3://")
	}

	path := strings.TrimPrefix(blockPath, "s3://")
	parts := strings.SplitN(path, "/", 3)
	if len(parts) < 3 {
		return "", "", "", fmt.Errorf("block path must be in format s3://bucket/tenant/block-id")
	}

	return parts[0], parts[1], parts[2], nil
}

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
	flag.IntVar(&cfg.ChunkTimeout, "chunk-timeout", 0, "Timeout per chunk in seconds (default: auto-calculated based on chunk size)")
	flag.StringVar(&cfg.WorkingDir, "working-dir", ".", "Working directory for caching downloaded files (default: current directory)")
	flag.Int64Var(&cfg.StartTime, "start-time", 0, "Start time (Unix timestamp in milliseconds, optional)")
	flag.Int64Var(&cfg.EndTime, "end-time", 0, "End time (Unix timestamp in milliseconds, optional)")
	flag.StringVar(&cfg.OutputFormat, "output", "csv", "Output format: csv, json, or prometheus")
	flag.Float64Var(&cfg.SwitchThreshold, "switch-threshold", 0.2, "Switch to full download when this fraction of index file is requested (0.1-0.9, default: 0.2)")
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

	// Read index to get chunk locations (with caching)
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

	// Set the switch threshold
	indexReader.threshold = cfg.SwitchThreshold
	
	// Force parallel download for index if requested
	if cfg.ForceIndexParallel {
		if cfg.Debug {
			fmt.Fprintf(os.Stderr, "Force parallel download requested for index file...\n")
		}
		if err := indexReader.downloadParallel(50 * 1024 * 1024); err != nil {
			return fmt.Errorf("failed to force download index file: %w", err)
		}
	}

	idx, err := index.NewReader(indexReader)
	if err != nil {
		return fmt.Errorf("failed to open index reader: %w", err)
	}
	defer idx.Close()

	// Get chunk locations from index
	fmt.Fprintf(os.Stderr, "Reading chunk locations from index...\n")
	chunkRefs, seriesLabels, err := getChunkReferences(*idx, cfg)
	if err != nil {
		return fmt.Errorf("failed to get chunk references: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Found %d chunks for %d series\n", len(chunkRefs), len(seriesLabels))

	// Read chunks file to get actual data (DON'T download the entire file)
	chunksKey := path.Join(tenant, blockID, "chunks", "000001")
	chunkFileName := fmt.Sprintf("%s/chunks/000001", blockID)
	
	if cfg.Debug {
		fmt.Fprintf(os.Stderr, "Setting up chunks reader for s3://%s/%s (chunk file: %s)\n", bucket, chunksKey, chunkFileName)
		if cfg.WorkingDir != "" {
			fmt.Fprintf(os.Stderr, "Using working directory for caching: %s\n", cfg.WorkingDir)
		}
	}
	
	chunkCacheDir := ""
	if cfg.WorkingDir != "" {
		chunkCacheDir = cfg.WorkingDir
	}
	
	chunksReader, err := NewOptimizedS3ReaderWithCache(s3Client, bucket, chunksKey, cfg.Debug, chunkCacheDir, "chunks")
	if err != nil {
		return fmt.Errorf("failed to create chunks reader: %w", err)
	}

	if cfg.Debug {
		fmt.Fprintf(os.Stderr, "Chunks file size: %d bytes (%.2f GB)\n", 
			chunksReader.Size(), float64(chunksReader.Size())/(1024*1024*1024))
		if cfg.ChunkTimeout > 0 {
			fmt.Fprintf(os.Stderr, "Using fixed chunk timeout: %d seconds\n", cfg.ChunkTimeout)
		} else {
			fmt.Fprintf(os.Stderr, "Using adaptive chunk timeout: 30s base + 1s per KB chunk size (max 5min)\n")
		}
		fmt.Fprintf(os.Stderr, "Will process chunks using %d parallel workers with targeted range requests\n", cfg.ChunkWorkers)
	}

	fmt.Fprintf(os.Stderr, "Reading time series data from %s...\n", chunkFileName)
	points, err := readChunkData(chunksReader, chunkRefs, seriesLabels, cfg, chunkFileName)
	if err != nil {
		return fmt.Errorf("failed to read chunk data: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Extracted %d data points\n", len(points))

	// Output results
	return outputResults(points, cfg)
}

func getChunkReferences(idx index.Reader, cfg Config) ([]chunks.Meta, []labels.Labels, error) {
	var postings index.Postings
	var err error

	if cfg.MetricName != "" {
		postings, err = idx.Postings(labels.MetricName, cfg.MetricName)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get postings for metric %s: %w", cfg.MetricName, err)
		}
	} else {
		postings, err = idx.Postings(index.AllPostingsKey())
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get all postings: %w", err)
		}
	}

	if cfg.LabelKey != "" && cfg.LabelValue != "" {
		labelPostings, err := idx.Postings(cfg.LabelKey, cfg.LabelValue)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get postings for label %s=%s: %w", cfg.LabelKey, cfg.LabelValue, err)
		}
		postings = index.Intersect(postings, labelPostings)
	}

	var allChunks []chunks.Meta
	var allLabels []labels.Labels
	var lbls labels.Labels
	var chks []chunks.Meta

	for postings.Next() {
		seriesID := postings.At()
		
		var builder labels.ScratchBuilder
		if err := idx.Series(seriesID, &builder, &chks); err != nil {
			return nil, nil, fmt.Errorf("failed to get series %d: %w", seriesID, err)
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
			
			allChunks = append(allChunks, chk)
			allLabels = append(allLabels, lbls)
		}
	}

	return allChunks, allLabels, postings.Err()
}

func readChunkData(chunksReader *OptimizedS3Reader, chunkRefs []chunks.Meta, seriesLabels []labels.Labels, cfg Config, chunkFileName string) ([]SeriesPoint, error) {
	var allPoints []SeriesPoint
	startTime := time.Now()

	fmt.Fprintf(os.Stderr, "Processing %d chunks from %s using %d parallel workers...\n", len(chunkRefs), chunkFileName, cfg.ChunkWorkers)

	// Create job and result channels
	jobs := make(chan ChunkJob, len(chunkRefs))
	results := make(chan ChunkResult, len(chunkRefs))

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
		for i, chkMeta := range chunkRefs {
			jobs <- ChunkJob{
				Index:       i,
				ChunkRef:    chkMeta,
				SeriesLabel: seriesLabels[i],
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
			chunkOffset := uint64(result.ChunkRef.Ref) >> 32
			chunkLength := uint32(result.ChunkRef.Ref)
			fmt.Fprintf(os.Stderr, "\nWarning: chunk %d/%d from %s (offset: %d, length: %d) failed: %v\n", 
				result.Index+1, len(chunkRefs), chunkFileName, chunkOffset, chunkLength, result.Error)
		} else {
			allPoints = append(allPoints, result.Points...)
			totalPoints += len(result.Points)
		}

		// Show progress every 1000 chunks or every 10 seconds
		if processed%1000 == 0 || time.Since(lastProgressTime) > 10*time.Second {
			elapsed := time.Since(startTime)
			rate := float64(processed) / elapsed.Seconds()
			remaining := time.Duration(float64(len(chunkRefs)-processed)/rate) * time.Second
			progress := float64(processed) / float64(len(chunkRefs)) * 100
			
			fmt.Fprintf(os.Stderr, "\rChunk progress: %d/%d (%.1f%%) - Rate: %.0f chunks/sec - ETA: %v - Points: %d", 
				processed, len(chunkRefs), progress, rate, remaining, totalPoints)
			lastProgressTime = time.Now()
		}
	}

	// Clear progress line and show final stats
	elapsed := time.Since(startTime)
	fmt.Fprintf(os.Stderr, "\rCompleted processing %d chunks from %s in %v using %d workers - Extracted %d data points                    \n", 
		len(chunkRefs), chunkFileName, elapsed, cfg.ChunkWorkers, len(allPoints))

	return allPoints, nil
}

func processChunks(workerID int, jobs <-chan ChunkJob, results chan<- ChunkResult, chunksReader *OptimizedS3Reader, cfg Config, chunkFileName string) {
	for job := range jobs {
		points, err := processOneChunk(job, chunksReader, cfg, workerID, chunkFileName)
		results <- ChunkResult{
			Points:   points,
			Error:    err,
			Index:    job.Index,
			ChunkRef: job.ChunkRef,
		}
	}
}

func processOneChunk(job ChunkJob, chunksReader *OptimizedS3Reader, cfg Config, workerID int, chunkFileName string) ([]SeriesPoint, error) {
	// Read chunk data using the reference offset
	chunkOffset := uint64(job.ChunkRef.Ref) >> 32 // Higher 32 bits are offset
	chunkLength := uint32(job.ChunkRef.Ref)       // Lower 32 bits are length

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
	labelsStr := job.SeriesLabel.String()
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

func outputResults(points []SeriesPoint, cfg Config) error {
	switch cfg.OutputFormat {
	case "csv":
		return outputCSV(points)
	case "json":
		return outputJSON(points)
	case "prometheus":
		return outputPrometheus(points)
	default:
		return fmt.Errorf("unsupported output format: %s", cfg.OutputFormat)
	}
}

func outputCSV(points []SeriesPoint) error {
	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()

	if err := writer.Write([]string{"series_labels", "timestamp", "value"}); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	for _, point := range points {
		record := []string{
			point.SeriesLabels,
			strconv.FormatInt(point.Timestamp, 10),
			strconv.FormatFloat(point.Value, 'f', -1, 64),
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write CSV record: %w", err)
		}
	}

	return nil
}

func outputJSON(points []SeriesPoint) error {
	fmt.Println("[")
	for i, point := range points {
		if i > 0 {
			fmt.Println(",")
		}
		fmt.Printf(`  {"series": %q, "timestamp": %d, "value": %g}`, 
			point.SeriesLabels, point.Timestamp, point.Value)
	}
	fmt.Println("\n]")
	return nil
}

func outputPrometheus(points []SeriesPoint) error {
	for _, point := range points {
		// Convert timestamp from milliseconds to seconds for Prometheus format
		timestampSec := float64(point.Timestamp) / 1000.0
		fmt.Printf("%s %g %g\n", point.SeriesLabels, point.Value, timestampSec)
	}
	return nil
}
