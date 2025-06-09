package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

func dumpSeriesLocal(cfg Config, blockDir, bucket, tenant, blockID string) error {
	if cfg.Debug {
		fmt.Fprintf(os.Stderr, "Reading local block at %s\n", blockDir)
	}
	blk, err := tsdb.OpenBlock(log.NewNopLogger(), blockDir, chunkenc.NewPool())
	if err != nil {
		return fmt.Errorf("open local block: %w", err)
	}
	defer blk.Close()

	idxr, err := blk.Index()
	if err != nil {
		return fmt.Errorf("block.Index: %w", err)
	}
	defer idxr.Close()

	chunkr, err := blk.Chunks()
	if err != nil {
		return fmt.Errorf("block.Chunks: %w", err)
	}
	defer chunkr.Close()

	postings, err := postingsFromConfig(idxr, cfg)
	if err != nil {
		return err
	}

	var builder labels.ScratchBuilder
	var metas []chunks.Meta
	var it chunkenc.Iterator

	var allPoints []SeriesPoint

	for postings.Next() {
		seriesID := postings.At()
		builder.Reset()
		metas = metas[:0]
		if err := idxr.Series(seriesID, &builder, &metas); err != nil {
			return fmt.Errorf("series %d: %w", seriesID, err)
		}
		lbls := builder.Labels()
		labelsStr := lbls.String()
		for _, meta := range metas {
			if cfg.StartTime > 0 && meta.MaxTime < cfg.StartTime {
				continue
			}
			if cfg.EndTime > 0 && meta.MinTime > cfg.EndTime {
				continue
			}
			chk, err := chunkr.Chunk(meta)
			if err != nil {
				return fmt.Errorf("chunk read: %w", err)
			}
			if chk == nil {
				continue
			}
			it = chk.Iterator(it)
			for it.Next() == chunkenc.ValFloat {
				ts, v := it.At()
				if cfg.StartTime > 0 && ts < cfg.StartTime {
					continue
				}
				if cfg.EndTime > 0 && ts > cfg.EndTime {
					continue
				}
				allPoints = append(allPoints, SeriesPoint{
					SeriesLabels: labelsStr,
					Labels:       lbls,
					Timestamp:    ts,
					Value:        v,
				})
			}
			if it.Err() != nil {
				return fmt.Errorf("iterator error: %w", it.Err())
			}
		}
	}
	if err := postings.Err(); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "Extracted %d data points from local block\n", len(allPoints))

	return outputResults(allPoints, cfg, bucket, tenant, blockID)
}

func postingsFromConfig(idx tsdb.IndexReader, cfg Config) (index.Postings, error) {
	var (
		p   index.Postings
		err error
	)
	if cfg.MetricName != "" {
		p, err = idx.Postings(labels.MetricName, cfg.MetricName)
	} else {
		p, err = idx.Postings(index.AllPostingsKey())
	}
	if err != nil {
		return nil, err
	}
	if cfg.LabelKey != "" && cfg.LabelValue != "" {
		lp, err := idx.Postings(cfg.LabelKey, cfg.LabelValue)
		if err != nil {
			return nil, err
		}
		p = index.Intersect(p, lp)
	}
	return p, nil
}

func localBlockPath(cfg Config, bucket, tenant, blockID string) string {
	return filepath.Join(cfg.WorkingDir, bucket, tenant, blockID)
}
