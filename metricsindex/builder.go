package metricsindex

import (
	"encoding/csv"
	"io"
	"sort"

	"github.com/prometheus/prometheus/tsdb/index"
)

// MetricsIndex represents a mapping of metric names to the labels they use.
type MetricsIndex struct {
	Labels       []string
	MetricLabels map[string]map[string]struct{}
}

// Build reads a Prometheus block index and returns a MetricsIndex describing
// which labels appear for each metric.
func Build(data []byte) (*MetricsIndex, error) {
	idxr, err := index.NewReader(data)
	if err != nil {
		return nil, err
	}
	defer idxr.Close()

	allLabels := map[string]struct{}{}
	metricLabels := map[string]map[string]struct{}{}

	it := idxr.Postings(index.AllPostingsKey())
	for it.Next() {
		id := it.At()
		lset, err := idxr.Labels(id)
		if err != nil {
			return nil, err
		}

		var metric string
		for _, l := range lset {
			allLabels[l.Name] = struct{}{}
			if l.Name == "__name__" {
				metric = l.Value
			}
		}

		if metric == "" {
			continue
		}

		ml, ok := metricLabels[metric]
		if !ok {
			ml = map[string]struct{}{}
			metricLabels[metric] = ml
		}
		for _, l := range lset {
			if l.Name == "__name__" {
				continue
			}
			ml[l.Name] = struct{}{}
		}
	}
	if it.Err() != nil {
		return nil, it.Err()
	}

	delete(allLabels, "__name__")

	labels := make([]string, 0, len(allLabels))
	for l := range allLabels {
		labels = append(labels, l)
	}
	sort.Strings(labels)

	return &MetricsIndex{
		Labels:       labels,
		MetricLabels: metricLabels,
	}, nil
}

// WriteCSV writes the MetricsIndex to w in CSV format with a header row
// containing the metric name column and all label names.
func WriteCSV(w io.Writer, idx *MetricsIndex) error {
	cw := csv.NewWriter(w)

	header := append([]string{"metric_name"}, idx.Labels...)
	if err := cw.Write(header); err != nil {
		return err
	}

	var metrics []string
	for m := range idx.MetricLabels {
		metrics = append(metrics, m)
	}
	sort.Strings(metrics)

	for _, m := range metrics {
		row := make([]string, 0, len(idx.Labels)+1)
		row = append(row, m)
		ml := idx.MetricLabels[m]
		for _, l := range idx.Labels {
			if _, ok := ml[l]; ok {
				row = append(row, "true")
			} else {
				row = append(row, "false")
			}
		}
		if err := cw.Write(row); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}
