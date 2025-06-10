package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/prometheus/prometheus/tsdb/index"
)

// LocalPieceReader reads file data stored as pieces on disk.
type LocalPieceReader struct {
	dir      string
	size     int64
	fileType string
	debug    bool
	cache    map[string][]byte
	mu       sync.RWMutex
	data     []byte
	useFull  bool
}

func NewLocalPieceReader(path string, fileType string, debug bool) (*LocalPieceReader, error) {
	info, err := os.Stat(path)
	if err == nil && !info.IsDir() {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		return &LocalPieceReader{
			dir:      filepath.Dir(path),
			size:     info.Size(),
			fileType: fileType,
			debug:    debug,
			data:     b,
			useFull:  true,
		}, nil
	}

	size, err := computeSizeFromPieces(path)
	if err != nil {
		return nil, err
	}

	return &LocalPieceReader{
		dir:      path,
		size:     size,
		fileType: fileType,
		debug:    debug,
		cache:    make(map[string][]byte),
	}, nil
}

func computeSizeFromPieces(dir string) (int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	var max int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var off, length int64
		if _, err := fmt.Sscanf(e.Name(), "%d_%d.bin", &off, &length); err == nil {
			if off+length > max {
				max = off + length
			}
		}
	}
	return max, nil
}

func (r *LocalPieceReader) pieceSize() int64 {
	if r.fileType == "index" {
		return indexPieceSize
	}
	return chunkPieceSize
}

func (r *LocalPieceReader) loadPiece(start, length int64) ([]byte, error) {
	key := fmt.Sprintf("%d_%d", start, length)
	r.mu.RLock()
	if data, ok := r.cache[key]; ok {
		r.mu.RUnlock()
		return data, nil
	}
	r.mu.RUnlock()

	file := filepath.Join(r.dir, fmt.Sprintf("%d_%d.bin", start, length))
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.cache[key] = data
	r.mu.Unlock()
	return data, nil
}

func (r *LocalPieceReader) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.size {
		return 0, io.EOF
	}
	end := off + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}

	if r.useFull {
		n := copy(p, r.data[off:end+1])
		if n < len(p) {
			return n, io.EOF
		}
		return n, nil
	}

	pieceSize := r.pieceSize()
	bytesRead := 0
	cur := off
	for cur <= end {
		pieceStart := (cur / pieceSize) * pieceSize
		pieceLen := pieceSize
		if pieceStart+pieceLen > r.size {
			pieceLen = r.size - pieceStart
		}
		data, err := r.loadPiece(pieceStart, pieceLen)
		if err != nil {
			return bytesRead, err
		}
		copyStart := cur - pieceStart
		copyLen := pieceLen - copyStart
		if cur+copyLen-1 > end {
			copyLen = end - cur + 1
		}
		if copyStart+copyLen > int64(len(data)) {
			copyLen = int64(len(data)) - copyStart
		}
		copy(p[bytesRead:bytesRead+int(copyLen)], data[copyStart:copyStart+copyLen])
		bytesRead += int(copyLen)
		cur += copyLen
	}
	if bytesRead < len(p) {
		return bytesRead, io.EOF
	}
	return bytesRead, nil
}

func (r *LocalPieceReader) Len() int { return int(r.size) }

func (r *LocalPieceReader) Range(start, end int) []byte {
	if start < 0 || int64(end) > r.size || start >= end {
		return nil
	}
	buf := make([]byte, end-start)
	n, _ := r.ReadAt(buf, int64(start))
	return buf[:n]
}

func (r *LocalPieceReader) Sub(start, end int) index.ByteSlice {
	if start < 0 || int64(end) > r.size || start >= end {
		return nil
	}
	data := r.Range(start, end)
	return &simpleByteSlice{data: data}
}

func (r *LocalPieceReader) Size() int64 { return r.size }
