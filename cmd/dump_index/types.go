package main

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
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
	DumpChunkTable     bool
}

type SeriesPoint struct {
	SeriesLabels string
	Timestamp    int64
	Value        float64
}

type ChunkInfo struct {
	ChunkRef     chunks.Meta
	SeriesLabel  labels.Labels
	ChunkFileNum int
	ChunkOffset  uint64
	ChunkLength  uint32
}

type ChunkJob struct {
	Index     int
	ChunkInfo ChunkInfo
}

type ChunkResult struct {
	Points    []SeriesPoint
	Error     error
	Index     int
	ChunkInfo ChunkInfo
}
