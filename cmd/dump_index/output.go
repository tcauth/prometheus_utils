package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func buildOutputPath(cfg Config, bucket, tenant, blockID, ext string) (string, error) {
	metric := cfg.MetricName
	if metric == "" {
		metric = "all-metrics"
	}

	name := cfg.OutputFilename
	if name == "" {
		rand.Seed(time.Now().UnixNano())
		name = fmt.Sprintf("%04d", rand.Intn(10000))
	}

	dir := filepath.Join(cfg.WorkingDir, bucket, tenant, blockID, metric)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}

	return filepath.Join(dir, name+"."+ext), nil
}

func outputResults(points []SeriesPoint, cfg Config, bucket, tenant, blockID string) error {
	ext := cfg.OutputFormat
	if ext == "prometheus" {
		ext = "prom"
	}

	outputPath, err := buildOutputPath(cfg, bucket, tenant, blockID, ext)
	if err != nil {
		return fmt.Errorf("failed to build output path: %w", err)
	}

	f, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer f.Close()

	switch cfg.OutputFormat {
	case "csv":
		err = outputCSV(f, points, cfg)
	case "json":
		err = outputJSON(f, points)
	case "prometheus":
		err = outputPrometheus(f, points)
	default:
		err = fmt.Errorf("unsupported output format: %s", cfg.OutputFormat)
	}

	if err == nil {
		fmt.Fprintf(os.Stderr, "Saved output to %s\n", outputPath)
	}

	return err
}

func outputCSV(w io.Writer, points []SeriesPoint, cfg Config) error {
	writer := csv.NewWriter(w)
	defer writer.Flush()

	if cfg.OutputLabels == "" {
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

	labelNames := []string{"__name__"}
	for _, l := range strings.Split(cfg.OutputLabels, ",") {
		name := strings.TrimSpace(l)
		if name == "" || name == "__name__" {
			continue
		}
		labelNames = append(labelNames, name)
	}

	header := append(labelNames, "timestamp", "value")
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	for _, point := range points {
		record := make([]string, len(labelNames)+2)
		for i, name := range labelNames {
			record[i] = point.Labels.Get(name)
		}
		record[len(labelNames)] = strconv.FormatInt(point.Timestamp, 10)
		record[len(labelNames)+1] = strconv.FormatFloat(point.Value, 'f', -1, 64)
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write CSV record: %w", err)
		}
	}

	return nil
}

func outputJSON(w io.Writer, points []SeriesPoint) error {
	fmt.Fprintln(w, "[")
	for i, point := range points {
		if i > 0 {
			fmt.Fprintln(w, ",")
		}
		fmt.Fprintf(w, `  {"series": %q, "timestamp": %d, "value": %g}`,
			point.SeriesLabels, point.Timestamp, point.Value)
	}
	fmt.Fprintln(w, "\n]")
	return nil
}

func outputPrometheus(w io.Writer, points []SeriesPoint) error {
	for _, point := range points {
		// Convert timestamp from milliseconds to seconds for Prometheus format
		timestampSec := float64(point.Timestamp) / 1000.0
		fmt.Fprintf(w, "%s %g %g\n", point.SeriesLabels, point.Value, timestampSec)
	}
	return nil
}
