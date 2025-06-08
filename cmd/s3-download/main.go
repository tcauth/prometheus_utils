package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Config struct {
	S3Path     string
	LocalPath  string
	AWSProfile string
	AWSRegion  string
}

func main() {
	var cfg Config

	flag.StringVar(&cfg.S3Path, "s3-path", "", "S3 path in format s3://bucket/key")
	flag.StringVar(&cfg.LocalPath, "local-path", "", "Local file path to save the downloaded file")
	flag.StringVar(&cfg.AWSProfile, "aws-profile", "", "AWS profile name (optional)")
	flag.StringVar(&cfg.AWSRegion, "aws-region", "", "AWS region (optional, will use profile default)")
	flag.Parse()

	if cfg.S3Path == "" {
		log.Fatal("S3 path is required (format: s3://bucket/key)")
	}
	if cfg.LocalPath == "" {
		log.Fatal("Local path is required")
	}

	if err := downloadFile(cfg); err != nil {
		log.Fatal(err)
	}
}

func parseS3Path(s3Path string) (bucket, key string, err error) {
	if !strings.HasPrefix(s3Path, "s3://") {
		return "", "", fmt.Errorf("S3 path must start with s3://")
	}

	path := strings.TrimPrefix(s3Path, "s3://")
	parts := strings.SplitN(path, "/", 2)
	
	if len(parts) < 2 {
		return "", "", fmt.Errorf("S3 path must include both bucket and key")
	}

	return parts[0], parts[1], nil
}

func downloadFile(cfg Config) error {
	// Parse S3 path
	bucket, key, err := parseS3Path(cfg.S3Path)
	if err != nil {
		return fmt.Errorf("invalid S3 path: %w", err)
	}

	fmt.Printf("Bucket: %s\n", bucket)
	fmt.Printf("Key: %s\n", key)
	fmt.Printf("Local path: %s\n", cfg.LocalPath)

	// Prepare AWS config options
	var configOpts []func(*config.LoadOptions) error

	// Add profile if specified
	if cfg.AWSProfile != "" {
		fmt.Printf("Using AWS profile: %s\n", cfg.AWSProfile)
		configOpts = append(configOpts, config.WithSharedConfigProfile(cfg.AWSProfile))
	}

	// Add region if specified
	if cfg.AWSRegion != "" {
		fmt.Printf("Using AWS region: %s\n", cfg.AWSRegion)
		configOpts = append(configOpts, config.WithRegion(cfg.AWSRegion))
	}

	// Load AWS configuration
	awsCfg, err := config.LoadDefaultConfig(context.Background(), configOpts...)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Display the region being used
	fmt.Printf("AWS Region: %s\n", awsCfg.Region)

	// Create S3 client
	s3Client := s3.NewFromConfig(awsCfg)

	// Test AWS credentials by getting caller identity (if available)
	fmt.Println("Testing AWS credentials...")

	// Get object metadata first
	fmt.Println("Getting object metadata...")
	headResp, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to get object metadata: %w", err)
	}

	fileSize := *headResp.ContentLength
	fmt.Printf("Object size: %d bytes (%.2f MB)\n", fileSize, float64(fileSize)/(1024*1024))
	
	if headResp.LastModified != nil {
		fmt.Printf("Last modified: %s\n", headResp.LastModified.Format(time.RFC3339))
	}

	// Create local directory if it doesn't exist
	localDir := filepath.Dir(cfg.LocalPath)
	if localDir != "." && localDir != "" {
		if err := os.MkdirAll(localDir, 0755); err != nil {
			return fmt.Errorf("failed to create local directory: %w", err)
		}
	}

	// Download the object
	fmt.Println("Downloading object...")
	startTime := time.Now()

	getResp, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to download object: %w", err)
	}
	defer getResp.Body.Close()

	// Create local file
	localFile, err := os.Create(cfg.LocalPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer localFile.Close()

	// Copy data with progress tracking
	bytesWritten, err := copyWithProgress(localFile, getResp.Body, fileSize)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	duration := time.Since(startTime)
	speedMBps := float64(bytesWritten) / (1024 * 1024) / duration.Seconds()

	fmt.Printf("\nDownload completed successfully!\n")
	fmt.Printf("Downloaded: %d bytes\n", bytesWritten)
	fmt.Printf("Duration: %v\n", duration)
	fmt.Printf("Speed: %.2f MB/s\n", speedMBps)
	fmt.Printf("Saved to: %s\n", cfg.LocalPath)

	return nil
}

func copyWithProgress(dst io.Writer, src io.Reader, totalSize int64) (int64, error) {
	buf := make([]byte, 32*1024) // 32KB buffer
	var written int64
	var lastPrint time.Time

	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = fmt.Errorf("invalid write result")
				}
			}
			written += int64(nw)
			if ew != nil {
				return written, ew
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}

			// Print progress every second
			if time.Since(lastPrint) > time.Second {
				progress := float64(written) / float64(totalSize) * 100
				fmt.Printf("\rProgress: %.1f%% (%d/%d bytes)", progress, written, totalSize)
				lastPrint = time.Now()
			}
		}
		if er != nil {
			if er != io.EOF {
				return written, er
			}
			break
		}
	}

	// Final progress update
	if totalSize > 0 {
		fmt.Printf("\rProgress: 100.0%% (%d/%d bytes)", written, totalSize)
	}

	return written, nil
}
