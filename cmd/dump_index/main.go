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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

type Config struct {
	BlockPath   string
	MetricName  string
	LabelKey    string
	LabelValue  string
	AWSRegion   string
	AWSProfile  string
	Debug       bool
}

type ChunkInfo struct {
	SeriesLabels string
	ChunkOffset  uint64
	ChunkSize    uint32
}

type OptimizedS3Reader struct {
	client    *s3.Client
	bucket    string
	key       string
	size      int64
	cache     map[string][]byte // Cache for previously read ranges
	debug     bool
}

func parseBlockPath(blockPath string) (bucket, tenant, blockID string, err error) {
	if !strings.HasPrefix(blockPath, "s3://") {
		return "", "", "", fmt.Errorf("block path must start with s3://")
	}

	// Remove s3:// prefix
	path := strings.TrimPrefix(blockPath, "s3://")
	
	// Split into parts: bucket/tenant/block-id
	parts := strings.SplitN(path, "/", 3)
	if len(parts) < 3 {
		return "", "", "", fmt.Errorf("block path must be in format s3://bucket/tenant/block-id")
	}

	return parts[0], parts[1], parts[2], nil
}

func NewOptimizedS3Reader(client *s3.Client, bucket, key string, debug bool) (*OptimizedS3Reader, error) {
	// Get object size
	headResp, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object info: %w", err)
	}

	return &OptimizedS3Reader{
		client: client,
		bucket: bucket,
		key:    key,
		size:   *headResp.ContentLength,
		cache:  make(map[string][]byte),
		debug:  debug,
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

	// Create cache key for this range
	cacheKey := fmt.Sprintf("%d-%d", off, end)
	
	// Check cache first
	if data, exists := r.cache[cacheKey]; exists {
		if r.debug {
			fmt.Fprintf(os.Stderr, "Cache hit for range %s (%d bytes)\n", cacheKey, len(data))
		}
		copy(p, data)
		return len(data), nil
	}

	// Optimize range size - read in larger chunks to reduce S3 requests
	// Round down to 64KB boundary for start, round up for end
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

	// Read the entire optimized range
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read response body: %w", err)
	}

	// Cache the optimized range
	optimizedCacheKey := fmt.Sprintf("%d-%d", optimizedStart, optimizedEnd)
	r.cache[optimizedCacheKey] = data

	// Extract the requested portion
	requestedOffset := off - optimizedStart
	requestedLength := end - off + 1
	
	if requestedOffset+requestedLength > int64(len(data)) {
		requestedLength = int64(len(data)) - requestedOffset
	}

	copy(p, data[requestedOffset:requestedOffset+requestedLength])
	return int(requestedLength), nil
}

func (r *OptimizedS3Reader) Size() int64 {
	return r.size
}

// Implement index.ByteSlice interface
func (r *OptimizedS3Reader) Len() int {
	return int(r.size)
}

func (r *OptimizedS3Reader) Range(start, end int) []byte {
	if start < 0 || end > int(r.size) || start >= end {
		return nil
	}
	
	data := make([]byte, end-start)
	n, err := r.ReadAt(data, int64(start))
	if err != nil && err != io.EOF {
		return nil
	}
	
	return data[:n]
}

func (r *OptimizedS3Reader) Sub(start, end int) index.ByteSlice {
	// For simplicity, we'll return a new reader for the sub-range
	// This is not the most efficient implementation but works for TSDB
	if start < 0 || end > int(r.size) || start >= end {
		return nil
	}
	
	// Create a simple byte slice implementation
	data := r.Range(start, end)
	return &simpleByteSlice{data: data}
}

// Simple implementation of index.ByteSlice for sub-ranges
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

func (r *OptimizedS3Reader) GetStats() (int, int) {
	totalRanges := len(r.cache)
	totalBytes := 0
	for _, data := range r.cache {
		totalBytes += len(data)
	}
	return totalRanges, totalBytes
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
	flag.Parse()

	if cfg.BlockPath == "" {
		log.Fatal("Block path is required (format: s3://bucket/tenant/block-id)")
	}

	if err := dumpBlock(cfg); err != nil {
		log.Fatal(err)
	}
}

func dumpBlock(cfg Config) error {
	// Parse the block path
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

	// Initialize AWS S3 client
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
		// Note: UseDualStack was renamed to UseDualstack in newer versions
		// Commenting out for compatibility
		// o.UseDualStack = true
		// o.UseAccelerate = true
	})

	// Construct S3 key for the index file: tenant/block-id/index
	indexKey := path.Join(tenant, blockID, "index")
	
	if cfg.Debug {
		fmt.Fprintf(os.Stderr, "Reading index from s3://%s/%s\n", bucket, indexKey)
	}

	// Create optimized S3 reader for the index file
	s3Reader, err := NewOptimizedS3Reader(s3Client, bucket, indexKey, cfg.Debug)
	if err != nil {
		return fmt.Errorf("failed to create S3 reader: %w", err)
	}

	// Open index reader - use NewReader instead of NewFileReader for custom readers
	indexReader, err := index.NewReader(s3Reader)
	if err != nil {
		return fmt.Errorf("failed to open index reader: %w", err)
	}
	defer indexReader.Close()

	if cfg.Debug {
		fmt.Fprintf(os.Stderr, "Index file size: %d bytes\n", s3Reader.Size())
	}

	// Build posting lists for efficient filtering
	var postings index.Postings
	
	if cfg.MetricName != "" {
		if cfg.Debug {
			fmt.Fprintf(os.Stderr, "Getting postings for metric: %s\n", cfg.MetricName)
		}
		metricPostings, err := indexReader.Postings(labels.MetricName, cfg.MetricName)
		if err != nil {
			return fmt.Errorf("failed to get postings for metric %s: %w", cfg.MetricName, err)
		}
		postings = metricPostings
	} else {
		if cfg.Debug {
			fmt.Fprintf(os.Stderr, "Getting all postings\n")
		}
		allPostings, err := indexReader.Postings(index.AllPostingsKey())
		if err != nil {
			return fmt.Errorf("failed to get all postings: %w", err)
		}
		postings = allPostings
	}

	// Additional label filtering if specified
	if cfg.LabelKey != "" && cfg.LabelValue != "" {
		if cfg.Debug {
			fmt.Fprintf(os.Stderr, "Applying label filter: %s=%s\n", cfg.LabelKey, cfg.LabelValue)
		}
		labelPostings, err := indexReader.Postings(cfg.LabelKey, cfg.LabelValue)
		if err != nil {
			return fmt.Errorf("failed to get postings for label %s=%s: %w", cfg.LabelKey, cfg.LabelValue, err)
		}
		postings = index.Intersect(postings, labelPostings)
	}

	// Collect chunk information
	var chunkInfos []ChunkInfo
	var lbls labels.Labels
	var chks []chunks.Meta
	seriesCount := 0

	if cfg.Debug {
		fmt.Fprintf(os.Stderr, "Processing matching series...\n")
	}

	for postings.Next() {
		seriesID := postings.At()
		seriesCount++
		
		if cfg.Debug && seriesCount%1000 == 0 {
			fmt.Fprintf(os.Stderr, "Processed %d series...\n", seriesCount)
		}
		
		// Get series labels and chunks - use ScratchBuilder for newer API
		var builder labels.ScratchBuilder
		if err := indexReader.Series(seriesID, &builder, &chks); err != nil {
			return fmt.Errorf("failed to get series %d: %w", seriesID, err)
		}
		
		// Get the labels from the builder
		lbls = builder.Labels()

		// Convert labels to string representation
		labelsStr := lbls.String()

		// Collect chunk information for this series
		for _, chk := range chks {
			// For TSDB chunks, the size isn't directly available from the index
			// The chunk reference (Ref) contains the offset, but size needs to be
			// calculated or estimated. For now, we'll use 0 as a placeholder
			// since the main purpose is to get the offset information.
			chunkSize := uint32(0) // Size not available from index alone
			
			chunkInfos = append(chunkInfos, ChunkInfo{
				SeriesLabels: labelsStr,
				ChunkOffset:  uint64(chk.Ref),
				ChunkSize:    chunkSize,
			})
		}
	}

	if err := postings.Err(); err != nil {
		return fmt.Errorf("postings iteration error: %w", err)
	}

	// Output statistics if debug is enabled
	if cfg.Debug {
		ranges, bytes := s3Reader.GetStats()
		fmt.Fprintf(os.Stderr, "\nS3 Transfer Statistics:\n")
		fmt.Fprintf(os.Stderr, "- Total series processed: %d\n", seriesCount)
		fmt.Fprintf(os.Stderr, "- Total chunks found: %d\n", len(chunkInfos))
		fmt.Fprintf(os.Stderr, "- S3 range requests made: %d\n", ranges)
		fmt.Fprintf(os.Stderr, "- Total bytes downloaded: %d (%.2f%% of index file)\n", 
			bytes, float64(bytes)/float64(s3Reader.Size())*100)
		fmt.Fprintf(os.Stderr, "\n")
	}

	// Output as CSV
	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()

	// Write header
	if err := writer.Write([]string{"series_labels", "chunk_offset", "chunk_size"}); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Write data
	for _, info := range chunkInfos {
		record := []string{
			info.SeriesLabels,
			strconv.FormatUint(info.ChunkOffset, 10),
			strconv.FormatUint(uint64(info.ChunkSize), 10),
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write CSV record: %w", err)
		}
	}

	if !cfg.Debug {
		fmt.Fprintf(os.Stderr, "Found %d chunks matching criteria\n", len(chunkInfos))
	}

	return nil
}
