package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Config struct {
	S3Path       string
	LocalPath    string
	AWSProfile   string
	AWSRegion    string
	CheckRegion  bool
	Parallel     bool
	ChunkSize    int64
}

func main() {
	var cfg Config

	flag.StringVar(&cfg.S3Path, "s3-path", "", "S3 path in format s3://bucket/key")
	flag.StringVar(&cfg.LocalPath, "local-path", "", "Local file path to save the downloaded file")
	flag.StringVar(&cfg.AWSProfile, "aws-profile", "", "AWS profile name (optional)")
	flag.StringVar(&cfg.AWSRegion, "aws-region", "", "AWS region (optional, will use profile default)")
	flag.BoolVar(&cfg.CheckRegion, "check-region", false, "Only check and display the bucket's region, don't download")
	flag.BoolVar(&cfg.Parallel, "parallel", true, "Use parallel download for better performance (default: true)")
	flag.Int64Var(&cfg.ChunkSize, "chunk-size", 10*1024*1024, "Chunk size for parallel downloads in bytes (default: 10MB)")
	flag.Parse()

	if cfg.S3Path == "" {
		log.Fatal("S3 path is required (format: s3://bucket/key)")
	}
	if cfg.LocalPath == "" && !cfg.CheckRegion {
		log.Fatal("Local path is required (unless using -check-region)")
	}

	if err := downloadFile(cfg); err != nil {
		log.Fatal(err)
	}
}

func testNetworkConnectivity(region string) error {
	// Test DNS resolution for S3 endpoint
	s3Endpoint := fmt.Sprintf("s3.%s.amazonaws.com", region)
	fmt.Printf("Testing DNS resolution for: %s\n", s3Endpoint)
	
	ips, err := net.LookupIP(s3Endpoint)
	if err != nil {
		return fmt.Errorf("DNS lookup failed for %s: %w", s3Endpoint, err)
	}
	
	fmt.Printf("DNS resolution successful. IPs: ")
	for i, ip := range ips {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Print(ip.String())
	}
	fmt.Println()
	
	// Test basic TCP connectivity
	fmt.Printf("Testing TCP connectivity to %s:443...\n", s3Endpoint)
	conn, err := net.DialTimeout("tcp", s3Endpoint+":443", 10*time.Second)
	if err != nil {
		return fmt.Errorf("TCP connection failed: %w", err)
	}
	conn.Close()
	fmt.Println("TCP connectivity test successful!")
	
	return nil
}

func getBucketRegion(s3Client *s3.Client, bucket string, ctx context.Context) (string, error) {
	// Try to get bucket location
	locationResp, err := s3Client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get bucket location: %w", err)
	}
	
	// Handle the special case where us-east-1 returns empty
	if locationResp.LocationConstraint == "" {
		return "us-east-1", nil
	}
	
	return string(locationResp.LocationConstraint), nil
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

	// Add region if specified - if not specified, try to use a default
	if cfg.AWSRegion != "" {
		fmt.Printf("Using AWS region: %s\n", cfg.AWSRegion)
		configOpts = append(configOpts, config.WithRegion(cfg.AWSRegion))
	} else {
		// Set a default region if none is specified
		fmt.Println("No region specified, using default: us-east-1")
		configOpts = append(configOpts, config.WithRegion("us-east-1"))
	}

	// Load AWS configuration
	fmt.Println("Loading AWS configuration...")
	awsCfg, err := config.LoadDefaultConfig(context.Background(), configOpts...)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Display the region being used
	fmt.Printf("AWS Region: %s\n", awsCfg.Region)
	
	// Validate the region is not empty
	if awsCfg.Region == "" {
		return fmt.Errorf("AWS region is empty - please specify -aws-region or configure it in your AWS profile")
	}

	// Test network connectivity first
	fmt.Println("Testing network connectivity...")
	if err := testNetworkConnectivity(awsCfg.Region); err != nil {
		return fmt.Errorf("network connectivity test failed: %w", err)
	}

	// Create S3 client with explicit configuration for better performance
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		// Force path-style addressing to avoid DNS issues with bucket names containing dots
		o.UsePathStyle = true
		// Enable dual-stack for better performance
		o.UseDualStack = true
		// Enable accelerated endpoint if available
		o.UseAccelerate = true
	})

	// Test AWS credentials by getting caller identity (if available)
	fmt.Println("Testing AWS credentials...")

	// Try to list buckets first as a basic connectivity test
	fmt.Println("Testing S3 connectivity...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	_, err = s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		fmt.Printf("Warning: Failed to list buckets (this might be due to permissions): %v\n", err)
		fmt.Println("Continuing with object download attempt...")
	} else {
		fmt.Println("S3 connectivity test successful!")
	}

	// If only checking region, do that and exit
	if cfg.CheckRegion {
		fmt.Println("Checking bucket region...")
		bucketRegion, err := getBucketRegion(s3Client, bucket, ctx)
		if err != nil {
			return fmt.Errorf("failed to get bucket region: %w", err)
		}
		fmt.Printf("Bucket '%s' is in region: %s\n", bucket, bucketRegion)
		fmt.Printf("You should use: -aws-region %s\n", bucketRegion)
		return nil
	}

	// Get object metadata first
	fmt.Println("Getting object metadata...")
	headResp, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// Check if it's a 301 redirect error (wrong region)
		if strings.Contains(err.Error(), "301") {
			fmt.Printf("Got 301 redirect - bucket might be in different region than %s\n", awsCfg.Region)
			fmt.Println("Attempting to get bucket location...")
			
			// Try to get the bucket's actual region
			bucketRegion, regionErr := getBucketRegion(s3Client, bucket, ctx)
			if regionErr != nil {
				return fmt.Errorf("failed to get bucket region after 301 error: %w", regionErr)
			}
			
			fmt.Printf("Bucket is actually in region: %s\n", bucketRegion)
			fmt.Printf("Recreating S3 client for correct region...\n")
			
			// Create new config with correct region
			newConfigOpts := []func(*config.LoadOptions) error{
				config.WithRegion(bucketRegion),
			}
			if cfg.AWSProfile != "" {
				newConfigOpts = append(newConfigOpts, config.WithSharedConfigProfile(cfg.AWSProfile))
			}
			
			newAwsCfg, err := config.LoadDefaultConfig(context.Background(), newConfigOpts...)
			if err != nil {
				return fmt.Errorf("failed to load AWS config for correct region: %w", err)
			}
			
			// Create new S3 client with correct region
			s3Client = s3.NewFromConfig(newAwsCfg, func(o *s3.Options) {
				o.UsePathStyle = true
				o.UseDualStack = true
				o.UseAccelerate = true
			})
			
			// Retry HeadObject with correct region
			headResp, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				return fmt.Errorf("failed to get object metadata even with correct region %s: %w", bucketRegion, err)
			}
			fmt.Printf("Successfully connected using region: %s\n", bucketRegion)
		} else {
			return fmt.Errorf("failed to get object metadata: %w", err)
		}
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

	var bytesWritten int64
	var err error

	if cfg.Parallel && fileSize > cfg.ChunkSize {
		fmt.Printf("Using parallel download with chunk size: %d MB\n", cfg.ChunkSize/(1024*1024))
		bytesWritten, err = downloadParallel(s3Client, bucket, key, cfg.LocalPath, fileSize, cfg.ChunkSize)
	} else {
		fmt.Println("Using single-threaded download...")
		downloadCtx, downloadCancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer downloadCancel()

		getResp, getErr := s3Client.GetObject(downloadCtx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if getErr != nil {
			return fmt.Errorf("failed to download object: %w", getErr)
		}
		defer getResp.Body.Close()

		// Create local file
		localFile, createErr := os.Create(cfg.LocalPath)
		if createErr != nil {
			return fmt.Errorf("failed to create local file: %w", createErr)
		}
		defer localFile.Close()

		// Copy data with progress tracking
		bytesWritten, err = copyWithProgress(localFile, getResp.Body, fileSize)
	}
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

func downloadParallel(s3Client *s3.Client, bucket, key, localPath string, fileSize, chunkSize int64) (int64, error) {
	// Calculate number of chunks
	numChunks := (fileSize + chunkSize - 1) / chunkSize
	fmt.Printf("Downloading in %d parallel chunks...\n", numChunks)

	// Create the output file
	file, err := os.Create(localPath)
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Pre-allocate the file to avoid fragmentation
	if err := file.Truncate(fileSize); err != nil {
		return 0, fmt.Errorf("failed to allocate file space: %w", err)
	}

	// Progress tracking
	var totalWritten int64
	var mu sync.Mutex
	progressTicker := time.NewTicker(2 * time.Second)
	defer progressTicker.Stop()

	go func() {
		for range progressTicker.C {
			mu.Lock()
			written := totalWritten
			mu.Unlock()
			progress := float64(written) / float64(fileSize) * 100
			fmt.Printf("\rProgress: %.1f%% (%d/%d bytes)", progress, written, fileSize)
		}
	}()

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
			if end >= fileSize {
				end = fileSize - 1
			}

			// Download this chunk
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			rangeHeader := fmt.Sprintf("bytes=%d-%d", start, end)
			resp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
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

			// Write to the correct position in the file
			_, err = file.WriteAt(chunkData, start)
			if err != nil {
				errChan <- fmt.Errorf("failed to write chunk %d: %w", chunkNum, err)
				return
			}

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
			return totalWritten, err
		}
	}

	fmt.Printf("\rProgress: 100.0%% (%d/%d bytes)", totalWritten, fileSize)
	return totalWritten, nil
}

func copyWithProgress(dst io.Writer, src io.Reader, totalSize int64) (int64, error) {
	buf := make([]byte, 1024*1024) // 1MB buffer (much larger than 32KB)
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

			// Print progress every 2 seconds (less frequent updates)
			if time.Since(lastPrint) > 2*time.Second {
				progress := float64(written) / float64(totalSize) * 100
				speed := float64(written) / (1024 * 1024) / time.Since(time.Now().Add(-time.Duration(written*int64(time.Second))/1024/1024)).Seconds()
				fmt.Printf("\rProgress: %.1f%% (%d/%d bytes) Speed: %.2f MB/s", progress, written, totalSize, speed)
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
