package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/prometheus/tsdb/index"
)

const (
	chunkPieceSize   int64 = 16 * 1024 * 1024
	indexPieceSize   int64 = 64 * 1024 * 1024
	pieceWorkerLimit       = 20
)

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
	localCacheDir  string
	fileType       string
	indexRanges    map[int64][]byte // For accumulating index ranges
}

func NewOptimizedS3Reader(client *s3.Client, bucket, key string, debug bool) (*OptimizedS3Reader, error) {
	return NewOptimizedS3ReaderWithCache(client, bucket, key, debug, "", "")
}

func NewOptimizedS3ReaderWithCache(client *s3.Client, bucket, key string, debug bool, cacheDir, fileType string) (*OptimizedS3Reader, error) {
	var size int64

	// If we're dealing with an index file and caching is enabled, check for a
	// local copy before hitting S3. This avoids unnecessary downloads when the
	// index already exists in the working directory.
	if cacheDir != "" && fileType == "index" {
		pathParts := strings.Split(key, "/")
		var indexPath string
		if len(pathParts) >= 3 && pathParts[len(pathParts)-1] == "index" {
			indexPath = filepath.Join(cacheDir, bucket, strings.Join(pathParts[:len(pathParts)-1], string(filepath.Separator)), "index")
		} else {
			indexPath = filepath.Join(cacheDir, bucket, strings.ReplaceAll(key, "/", string(filepath.Separator)))
		}

		if stat, err := os.Stat(indexPath); err == nil {
			size = stat.Size()
			if debug {
				fmt.Fprintf(os.Stderr, "Using cached index at %s (%d bytes)\n", indexPath, size)
			}
			return &OptimizedS3Reader{
				client:        client,
				bucket:        bucket,
				key:           key,
				size:          size,
				cache:         make(map[string][]byte),
				debug:         debug,
				useFullData:   false,
				localCacheDir: cacheDir,
				fileType:      fileType,
				indexRanges:   make(map[int64][]byte),
			}, nil
		}
	}

	// No cached file found; fall back to S3 HEAD request to determine size
	headResp, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object info: %w", err)
	}
	size = *headResp.ContentLength

	return &OptimizedS3Reader{
		client:        client,
		bucket:        bucket,
		key:           key,
		size:          size,
		cache:         make(map[string][]byte),
		debug:         debug,
		useFullData:   false,
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
			// For chunk files, check if the exact range is cached
			cachedData, found := r.readChunkFromLocalCache(off, end-off+1)
			if found {
				copy(p, cachedData)
				return len(cachedData), nil
			}
		}
	}

	// All reads rely on the piece-based cache mechanism; we don't switch to full downloads

	// Continue with range requests
	if r.fileType == "chunks" {
		return r.downloadChunkPiecesParallel(ctx, p, off, end)
	}

	if r.fileType == "index" {
		return r.downloadIndexPiecesParallel(ctx, p, off, end)
	}

	r.mu.RLock()
	cacheKey := fmt.Sprintf("%d-%d", off, end)
	if data, exists := r.cache[cacheKey]; exists {
		r.mu.RUnlock()
		copy(p, data)
		return len(data), nil
	}
	r.mu.RUnlock()

	rangeStart := (off / 262144) * 262144
	rangeEnd := ((end/262144)+1)*262144 - 1
	if rangeEnd >= r.size {
		rangeEnd = r.size - 1
	}

	rangeHeader := fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)

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

	if r.localCacheDir != "" {
		r.saveIndexRangeToLocalCache(rangeStart, data)
	}

	r.mu.Lock()
	optimizedCacheKey := fmt.Sprintf("%d-%d", rangeStart, rangeEnd)
	r.cache[optimizedCacheKey] = data
	r.totalRequested += int64(len(data))
	r.mu.Unlock()

	requestedOffset := off - rangeStart
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

func (r *OptimizedS3Reader) downloadPiecesParallel(ctx context.Context, p []byte, off, end, pieceSize int64) (int, error) {
	type piece struct {
		start  int64
		length int64
		data   []byte
	}

	var pieces []piece
	for pieceStart := (off / pieceSize) * pieceSize; pieceStart <= end; pieceStart += pieceSize {
		pieceLength := pieceSize
		if pieceStart+pieceLength > r.size {
			pieceLength = r.size - pieceStart
		}
		pieces = append(pieces, piece{start: pieceStart, length: pieceLength})
	}

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, pieceWorkerLimit)
	errChan := make(chan error, len(pieces))

	for i := range pieces {
		wg.Add(1)
		go func(pc *piece) {
			defer wg.Done()

			cacheKey := fmt.Sprintf("%d-%d", pc.start, pc.start+pc.length-1)
			r.mu.RLock()
			data, exists := r.cache[cacheKey]
			r.mu.RUnlock()

			if !exists {
				var found bool
				data, found = r.readChunkPieceFromLocalCache(pc.start, pc.length)
				if !found {
					semaphore <- struct{}{}
					rangeHeader := fmt.Sprintf("bytes=%d-%d", pc.start, pc.start+pc.length-1)
					resp, err := r.client.GetObject(ctx, &s3.GetObjectInput{
						Bucket: aws.String(r.bucket),
						Key:    aws.String(r.key),
						Range:  aws.String(rangeHeader),
					})
					if err != nil {
						<-semaphore
						errChan <- fmt.Errorf("failed to read range %s: %w", rangeHeader, err)
						return
					}
					data, err = io.ReadAll(resp.Body)
					resp.Body.Close()
					<-semaphore
					if err != nil {
						errChan <- fmt.Errorf("failed to read response body: %w", err)
						return
					}
					r.saveChunkPieceToLocalCache(pc.start, data)
				}
				r.mu.Lock()
				r.cache[cacheKey] = data
				r.totalRequested += int64(len(data))
				r.mu.Unlock()
			}

			pc.data = data
		}(&pieces[i])
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return 0, err
		}
	}

	bytesRead := 0
	cur := off
	for cur <= end {
		pieceStart := (cur / pieceSize) * pieceSize
		var pcData []byte
		for _, pc := range pieces {
			if pc.start == pieceStart {
				pcData = pc.data
				break
			}
		}

		if pcData == nil {
			return bytesRead, fmt.Errorf("missing piece data for offset %d", pieceStart)
		}

		copyStart := cur - pieceStart
		copyLen := int64(len(pcData)) - copyStart
		if cur+copyLen-1 > end {
			copyLen = end - cur + 1
		}
		copy(p[bytesRead:bytesRead+int(copyLen)], pcData[copyStart:copyStart+copyLen])
		bytesRead += int(copyLen)
		cur += copyLen
	}

	return bytesRead, nil
}

func (r *OptimizedS3Reader) downloadChunkPiecesParallel(ctx context.Context, p []byte, off, end int64) (int, error) {
	return r.downloadPiecesParallel(ctx, p, off, end, chunkPieceSize)
}

func (r *OptimizedS3Reader) downloadIndexPiecesParallel(ctx context.Context, p []byte, off, end int64) (int, error) {
	return r.downloadPiecesParallel(ctx, p, off, end, indexPieceSize)
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
		// For index files, the key is like "tenant/block-id/index"
		// We want: <working-dir>/<bucket>/<tenant>/<block-id>/index
		pathParts := strings.Split(r.key, "/")
		var indexPath string
		if len(pathParts) >= 3 && pathParts[len(pathParts)-1] == "index" {
			// key = "tenant/block-id/index" -> path = <working-dir>/<bucket>/<tenant>/<block-id>/index
			indexPath = filepath.Join(r.localCacheDir, r.bucket, strings.Join(pathParts[:len(pathParts)-1], string(filepath.Separator)), "index")
		} else {
			// fallback
			indexPath = filepath.Join(r.localCacheDir, r.bucket, strings.ReplaceAll(r.key, "/", string(filepath.Separator)))
		}

		if r.debug {
			fmt.Fprintf(os.Stderr, "Checking for cached index at: %s\n", indexPath)
		}

		if data, err := os.ReadFile(indexPath); err == nil {
			if int64(len(data)) >= offset+length {
				if r.debug {
					fmt.Fprintf(os.Stderr, "Index cache hit: %s (%d bytes)\n", indexPath, len(data))
				}
				return data[offset : offset+length], true
			}
		}
	} else if r.fileType == "chunks" {
		// For chunk files, the key is like "tenant/block-id/chunks/000001"
		// We want: <working-dir>/<bucket>/<tenant>/<block-id>/chunks/000001/<offset>_<length>.bin
		chunkCacheDir := filepath.Join(r.localCacheDir, r.bucket, strings.ReplaceAll(r.key, "/", string(filepath.Separator)))
		chunkFile := filepath.Join(chunkCacheDir, fmt.Sprintf("%d_%d.bin", offset, length))
		if data, err := os.ReadFile(chunkFile); err == nil {
			if r.debug {
				fmt.Fprintf(os.Stderr, "Chunk cache hit: %s\n", chunkFile)
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

	// For index files, the key is like "tenant/block-id/index"
	// We want: <working-dir>/<bucket>/<tenant>/<block-id>/index
	pathParts := strings.Split(r.key, "/")
	var indexPath string
	if len(pathParts) >= 3 && pathParts[len(pathParts)-1] == "index" {
		// key = "tenant/block-id/index" -> path = <working-dir>/<bucket>/<tenant>/<block-id>/index
		indexPath = filepath.Join(r.localCacheDir, r.bucket, strings.Join(pathParts[:len(pathParts)-1], string(filepath.Separator)), "index")
	} else {
		// fallback
		indexPath = filepath.Join(r.localCacheDir, r.bucket, strings.ReplaceAll(r.key, "/", string(filepath.Separator)))
	}

	if err := os.MkdirAll(filepath.Dir(indexPath), 0755); err != nil {
		if r.debug {
			fmt.Fprintf(os.Stderr, "Failed to create index cache directory: %v\n", err)
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

func (r *OptimizedS3Reader) readChunkFromLocalCache(offset int64, length int64) ([]byte, bool) {
	const pieceSize int64 = chunkPieceSize
	if r.localCacheDir == "" {
		return nil, false
	}

	end := offset + length - 1
	chunkCacheDir := filepath.Join(r.localCacheDir, r.bucket, strings.ReplaceAll(r.key, "/", string(filepath.Separator)))

	var result []byte
	for partStart := (offset / pieceSize) * pieceSize; partStart <= end; partStart += pieceSize {
		partLength := pieceSize
		if partStart+partLength > r.size {
			partLength = r.size - partStart
		}

		partFile := filepath.Join(chunkCacheDir, fmt.Sprintf("%d_%d.bin", partStart, partLength))
		data, err := os.ReadFile(partFile)
		if err != nil {
			return nil, false
		}

		var sliceStart int64
		if partStart < offset {
			sliceStart = offset - partStart
		}
		sliceEnd := partLength
		if partStart+partLength-1 > end {
			sliceEnd = end - partStart + 1
		}

		if sliceStart >= int64(len(data)) {
			return nil, false
		}
		if sliceStart+sliceEnd > int64(len(data)) {
			sliceEnd = int64(len(data)) - sliceStart
		}

		result = append(result, data[sliceStart:sliceStart+sliceEnd]...)
	}

	if int64(len(result)) == length {
		if r.debug {
			fmt.Fprintf(os.Stderr, "Chunk cache hit for offset %d length %d\n", offset, length)
		}
		return result, true
	}

	return nil, false
}

func (r *OptimizedS3Reader) saveExactChunkToLocalCache(offset int64, length int64, data []byte) {
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

	chunkFile := filepath.Join(chunkCacheDir, fmt.Sprintf("%d_%d.bin", offset, length))
	if err := os.WriteFile(chunkFile, data, 0644); err != nil {
		if r.debug {
			fmt.Fprintf(os.Stderr, "Failed to cache chunk piece: %v\n", err)
		}
	} else if r.debug {
		fmt.Fprintf(os.Stderr, "Cached chunk piece to: %s\n", chunkFile)
	}
}

func (r *OptimizedS3Reader) readChunkPieceFromLocalCache(offset int64, length int64) ([]byte, bool) {
	if r.localCacheDir == "" {
		return nil, false
	}

	chunkCacheDir := filepath.Join(r.localCacheDir, r.bucket, strings.ReplaceAll(r.key, "/", string(filepath.Separator)))
	chunkFile := filepath.Join(chunkCacheDir, fmt.Sprintf("%d_%d.bin", offset, length))
	data, err := os.ReadFile(chunkFile)
	if err == nil {
		if r.debug {
			fmt.Fprintf(os.Stderr, "Chunk piece cache hit: %s\n", chunkFile)
		}
		return data, true
	}
	return nil, false
}

func (r *OptimizedS3Reader) saveChunkPieceToLocalCache(offset int64, data []byte) {
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
			fmt.Fprintf(os.Stderr, "Failed to cache chunk piece: %v\n", err)
		}
	} else if r.debug {
		fmt.Fprintf(os.Stderr, "Cached chunk piece to: %s\n", chunkFile)
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

	if r.debug {
		fmt.Fprintf(os.Stderr, "Accumulated index range: offset=%d, length=%d (total ranges: %d)\n",
			offset, len(data), len(r.indexRanges))
	}

	// Check if we should try to reconstruct the full index
	// This is a simple heuristic - if we have enough data, try to reconstruct
	totalCachedBytes := int64(0)
	for _, rangeData := range r.indexRanges {
		totalCachedBytes += int64(len(rangeData))
	}

	coverage := float64(totalCachedBytes) / float64(r.size)
	if r.debug {
		fmt.Fprintf(os.Stderr, "Index coverage so far: %.1f%% (%d/%d bytes)\n",
			coverage*100, totalCachedBytes, r.size)
	}

	// If we have >= 90% of the file in ranges, reconstruct and cache the full index
	if coverage >= 0.9 {
		fmt.Fprintf(os.Stderr, "Triggering index reconstruction at %.1f%% coverage...\n", coverage*100)
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
		// For index files, the key is like "tenant/block-id/index"
		// We want: <working-dir>/<bucket>/<tenant>/<block-id>/index
		pathParts := strings.Split(r.key, "/")
		var indexPath string
		if len(pathParts) >= 3 && pathParts[len(pathParts)-1] == "index" {
			// key = "tenant/block-id/index" -> path = <working-dir>/<bucket>/<tenant>/<block-id>/index
			indexPath = filepath.Join(r.localCacheDir, r.bucket, strings.Join(pathParts[:len(pathParts)-1], string(filepath.Separator)), "index")
		} else {
			// fallback
			indexPath = filepath.Join(r.localCacheDir, r.bucket, strings.ReplaceAll(r.key, "/", string(filepath.Separator)))
		}

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
