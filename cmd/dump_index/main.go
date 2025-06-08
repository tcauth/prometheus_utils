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
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/labels"
)

type Config struct {
	BlockID     string
	MetricName  string
	LabelKey    string
	LabelValue  string
	S3Bucket    string
	S3Prefix    string
	AWSRegion   string
}

type ChunkInfo struct {
	SeriesLabels string
	ChunkOffset  uint64
	ChunkSize    uint32
}

type S3Reader struct {
	client *s3.Client
	bucket string
	key    string
	size   int64
}

func NewS3Reader(client *s3.Client, bucket, key string) (*S3Reader, error) {
	// Get object size
	headResp, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object info: %w", err)
	}

	return &S3Reader{
		client: client,
		bucket: bucket,
		key:    key,
		size:   *headResp.ContentLength,
	}, nil
}

func (r *S3Reader) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= r.size {
		return 0, io.EOF
	}

	end := off + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}

	rangeHeader := fmt.Sprintf("bytes=%d-%d", off, end)
	
	resp, err := r.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(r.key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to read range %s: %w", rangeHeader, err)
	}
	defer resp.Body.Close()

	return io.ReadFull(resp.Body, p)
}

func (r *S3Reader) Size() int64 {
	return r.size
}

func main() {
	var cfg Config
	
	flag.StringVar(&cfg.BlockID, "block", "", "Block ID to dump")
	flag.StringVar(&cfg.MetricName, "metric-name", "", "Metric name to filter (optional)")
	flag.StringVar(&cfg.LabelKey, "label-key", "", "Label key to filter (optional)")
	flag.StringVar(&cfg.LabelValue, "label-value", "", "Label value to filter (optional)")
	flag.StringVar(&cfg.S3Bucket, "s3-bucket", "", "S3 bucket name")
	flag.StringVar(&cfg.S3Prefix, "s3-prefix", "", "S3 prefix/path to blocks")
	flag.StringVar(&cfg.AWSRegion, "aws-region", "us-east-1", "AWS region")
	flag.Parse()

	if cfg.BlockID == "" {
		log.Fatal("Block ID is required")
	}
	if cfg.S3Bucket == "" {
		log.Fatal("S3 bucket is required")
	}

	if err := dumpBlock(cfg); err != nil {
		log.Fatal(err)
	}
}

func dumpBlock(cfg Config) error {
	// Initialize AWS S3 client
	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cfg.AWSRegion),
	)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsCfg)

	// Construct S3 key for the index file
	indexKey := path.Join(cfg.S3Prefix, cfg.BlockID, "index")
	
	// Create S3 reader for the index file
	s3Reader, err := NewS3Reader(s3Client, cfg.S3Bucket, indexKey)
	if err != nil {
		return fmt.Errorf("failed to create S3 reader: %w", err)
	}

	// Open index reader
	indexReader, err := index.NewFileReader(s3Reader)
	if err != nil {
		return fmt.Errorf("failed to open index reader: %w", err)
	}
	defer indexReader.Close()

	// Get all postings for the metric name if specified
	var postings index.Postings
	if cfg.MetricName != "" {
		postings, err = indexReader.Postings(labels.MetricName, cfg.MetricName)
		if err != nil {
			return fmt.Errorf("failed to get postings for metric %s: %w", cfg.MetricName, err)
		}
	} else {
		// Get all postings
		postings, err = indexReader.Postings(index.AllPostingsKey())
		if err != nil {
			return fmt.Errorf("failed to get all postings: %w", err)
		}
	}

	// Additional label filtering if specified
	if cfg.LabelKey != "" && cfg.LabelValue != "" {
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

	for postings.Next() {
		seriesID := postings.At()
		
		// Get series labels
		if err := indexReader.Series(seriesID, &lbls, &chks); err != nil {
			return fmt.Errorf("failed to get series %d: %w", seriesID, err)
		}

		// Convert labels to string representation
		labelsStr := lbls.String()

		// Collect chunk information for this series
		for _, chk := range chks {
			chunkInfos = append(chunkInfos, ChunkInfo{
				SeriesLabels: labelsStr,
				ChunkOffset:  uint64(chk.Ref),
				ChunkSize:    chk.Chunk.Len(),
			})
		}
	}

	if err := postings.Err(); err != nil {
		return fmt.Errorf("postings iteration error: %w", err)
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

	fmt.Fprintf(os.Stderr, "Found %d chunks matching criteria\n", len(chunkInfos))
	return nil
}
