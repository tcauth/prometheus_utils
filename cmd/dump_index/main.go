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
	BlockPath     string
	MetricName    string
	LabelKey      string
	LabelValue    string
	AWSRegion     string
	AWSProfile    string
	Debug         bool
	CheckRegion   bool
	ForceParallel bool
	StartTime     int64
	EndTime       int64
	OutputFormat  string
	SwitchThreshold float64
}

type SeriesPoint struct {
	SeriesLabels string
	Timestamp    int64
	Value        float64
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
	totalRequested int64  // Track how much data we've requested via ranges
	requestCount   int    // Track number of range requests
	threshold      float64 // When to switch to full download (e.g., 0.3 = 30%)
}

func NewOptimizedS3Reader(client *s3.Client, bucket, key string, debug bool) (*OptimizedS3Reader, error) {
	headResp, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object info: %w", err)
	}

	return &OptimizedS3Reader{
		client:      client,
		bucket:      bucket,
		key:         key,
		size:        *headResp.ContentLength,
		cache:       make(map[string][]byte),
		debug:       debug,
		useFullData: false,
		threshold:   0.3, // Switch to full download when we've requested 30% of the file
	}, nil
}

func (r *OptimizedS3Reader) ReadAt(p []byte, off int64) (n int, err error) {
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

	// Check if we should switch to full download
	r.mu.Lock()
	requestedDataRatio := float64(r.totalRequested) / float64(r.size)
	shouldSwitchToFull := requestedDataRatio > r.threshold || r.requestCount > 20
	r.mu.Unlock()

	if shouldSwitchToFull && !r.useFullData {
		if r.debug {
			fmt.Fprintf(os.Stderr, "Switching to full download: %.1f%% of file requested in %d requests\n", 
				requestedDataRatio*100, r.requestCount)
		}
		
		// Download the entire file using parallel download
		if err := r.downloadParallel(10 * 1024 * 1024); err != nil {
			if r.debug {
				fmt.Fprintf(os.Stderr, "Failed to download full file, continuing with range requests: %v\n", err)
			}
		} else {
			// Now use the full data
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
		if r.debug {
			fmt.Fprintf(os.Stderr, "Cache hit for range %s (%d bytes)\n", cacheKey, len(data))
		}
		copy(p, data)
		return len(data), nil
	}
	r.mu.RUnlock()

	// Optimize range size - read in larger chunks to reduce S3 requests
	optimizedStart := (off / 65536) * 65536
	optimizedEnd := ((end / 65536) + 1) * 65536 - 1
	if optimizedEnd >= r.size {
		optimizedEnd = r.size - 1
	}

	rangeHeader := fmt.Sprintf("bytes=%d-%d", optimizedStart, optimizedEnd)
	
	if r.debug {
		fmt.Fprintf(os.Stderr, "S3 request: %s (requested: %d-%d, optimized: %d-%d)\n", 
			rangeHeader, off, end, optimizedStart, optimizedEnd)
	}
	
	resp, err := r.client.GetObject(context.Background(), &s3.GetObjectInput{
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

	// Pre-allocate the data slice
	r.data = make([]byte, r.size)
	
	startTime := time.Now()
	
	// Progress tracking
	var totalWritten int64
	var mu sync.Mutex

	// Download chunks in parallel
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 10) // Limit to 10 concurrent downloads
	errChan := make(chan error, numChunks)

	for i := int64(0); i < numChunks; i++ {
		wg.Add(1)
		go func(chunkNum int64) {
			defer wg.Done()
			semaphore <- struct{}{} // Acquire
			defer func() { <-semaphore }() // Release

			start := chunkNum * chunkSize
			end := start + chunkSize - 1
			if end >= r.size {
				end = r.size - 1
			}

			// Download this chunk
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			rangeHeader := fmt.Sprintf("bytes=%d-%d", start, end)
			resp, err := r.client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(r.bucket),
				Key:    aws.String(r.key),
				Range:  aws.String(rangeHeader),
			})
			if err != nil {
				errChan <- fmt.Errorf("failed to download chunk %d: %w", chunkNum, err)
				return
			}
			defer resp.Body.Close()

			// Read the chunk data
			chunkData, err := io.ReadAll(resp.Body)
			if err != nil {
				errChan <- fmt.Errorf("failed to read chunk %d: %w", chunkNum, err)
				return
			}

			// Write to the correct position in the data slice
			copy(r.data[start:start+int64(len(chunkData))], chunkData)

			// Update progress
			mu.Lock()
			totalWritten += int64(len(chunkData))
			mu.Unlock()

		}(i)
	}

	// Wait for all chunks to complete
	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	r.useFullData = true
	
	elapsed := time.Since(startTime)
	rate := float64(r.size) / (1024 * 1024) / elapsed.Seconds()
	fmt.Fprintf(os.Stderr, "Full download completed: %d bytes in %v (%.2f MB/s)\n", 
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
	flag.Int64Var(&cfg.StartTime, "start-time", 0, "Start time (Unix timestamp in milliseconds, optional)")
	flag.Int64Var(&cfg.EndTime, "end-time", 0, "End time (Unix timestamp in milliseconds, optional)")
	flag.StringVar(&cfg.OutputFormat, "output", "csv", "Output format: csv, json, or prometheus")
	flag.Float64Var(&cfg.SwitchThreshold, "switch-threshold", 0.3, "Switch to full download when this fraction of file is requested (0.1-0.9)")
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

	// Read index to get chunk locations
	indexKey := path.Join(tenant, blockID, "index")
	indexReader, err := NewOptimizedS3Reader(s3Client, bucket, indexKey, cfg.Debug)
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
			
			indexReader, err = NewOptimizedS3Reader(s3Client, bucket, indexKey, cfg.Debug)
			if err != nil {
				return fmt.Errorf("failed to create index reader with correct region: %w", err)
			}
		} else {
			return fmt.Errorf("failed to create index reader: %w", err)
		}
	}

	// Set the switch threshold
	indexReader.threshold = cfg.SwitchThreshold

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

	// Read chunks file to get actual data
	chunksKey := path.Join(tenant, blockID, "chunks", "000001")
	chunksReader, err := NewOptimizedS3Reader(s3Client, bucket, chunksKey, cfg.Debug)
	if err != nil {
		return fmt.Errorf("failed to create chunks reader: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Reading time series data from chunks...\n")
	points, err := readChunkData(chunksReader, chunkRefs, seriesLabels, cfg)
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

func readChunkData(chunksReader *OptimizedS3Reader, chunkRefs []chunks.Meta, seriesLabels []labels.Labels, cfg Config) ([]SeriesPoint, error) {
	var points []SeriesPoint

	for i, chkMeta := range chunkRefs {
		if cfg.Debug && i%1000 == 0 {
			fmt.Fprintf(os.Stderr, "Processing chunk %d/%d...\n", i+1, len(chunkRefs))
		}

		// Read chunk data using the reference offset
		chunkOffset := uint64(chkMeta.Ref) >> 32 // Higher 32 bits are offset
		chunkLength := uint32(chkMeta.Ref)       // Lower 32 bits are length

		if chunkLength == 0 {
			continue // Skip if we can't determine chunk size
		}

		chunkData := make([]byte, chunkLength)
		n, err := chunksReader.ReadAt(chunkData, int64(chunkOffset))
		if err != nil && err != io.EOF {
			if cfg.Debug {
				fmt.Fprintf(os.Stderr, "Warning: failed to read chunk at offset %d: %v\n", chunkOffset, err)
			}
			continue
		}

		if n == 0 {
			continue
		}

		// Decode chunk
		chunk, err := chunkenc.FromData(chunkenc.EncXOR, chunkData[:n])
		if err != nil {
			if cfg.Debug {
				fmt.Fprintf(os.Stderr, "Warning: failed to decode chunk: %v\n", err)
			}
			continue
		}

		// Extract time series points
		iter := chunk.Iterator(nil)
		labelsStr := seriesLabels[i].String()

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
			if cfg.Debug {
				fmt.Fprintf(os.Stderr, "Warning: iterator error: %v\n", err)
			}
		}
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
