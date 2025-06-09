package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
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

type streamWriter interface {
	WritePoints([]SeriesPoint) error
	Close() error
}

func newStreamWriter(cfg Config, bucket, tenant, blockID string) (streamWriter, string, error) {
	ext := cfg.OutputFormat
	if ext == "prometheus" {
		ext = "prom"
	}

	outputPath, err := buildOutputPath(cfg, bucket, tenant, blockID, ext)
	if err != nil {
		return nil, "", fmt.Errorf("failed to build output path: %w", err)
	}

	f, err := os.Create(outputPath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create output file: %w", err)
	}

	switch cfg.OutputFormat {
	case "csv":
		w := csv.NewWriter(f)
		if err := w.Write([]string{"series_labels", "timestamp", "value"}); err != nil {
			f.Close()
			return nil, "", fmt.Errorf("failed to write CSV header: %w", err)
		}
		return &csvStreamWriter{f: f, w: w}, outputPath, nil
	case "json":
		if _, err := fmt.Fprintln(f, "["); err != nil {
			f.Close()
			return nil, "", err
		}
		return &jsonStreamWriter{f: f, first: true}, outputPath, nil
	case "prometheus":
		return &promStreamWriter{f: f}, outputPath, nil
	default:
		f.Close()
		return nil, "", fmt.Errorf("unsupported output format: %s", cfg.OutputFormat)
	}
}

type csvStreamWriter struct {
	f *os.File
	w *csv.Writer
}

func (c *csvStreamWriter) WritePoints(points []SeriesPoint) error {
	for _, p := range points {
		record := []string{
			p.SeriesLabels,
			strconv.FormatInt(p.Timestamp, 10),
			strconv.FormatFloat(p.Value, 'f', -1, 64),
		}
		if err := c.w.Write(record); err != nil {
			return err
		}
	}
	return nil
}

func (c *csvStreamWriter) Close() error {
	c.w.Flush()
	err1 := c.w.Error()
	err2 := c.f.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

type jsonStreamWriter struct {
	f     *os.File
	first bool
}

func (j *jsonStreamWriter) WritePoints(points []SeriesPoint) error {
	for _, p := range points {
		if j.first {
			j.first = false
		} else {
			if _, err := fmt.Fprintln(j.f, ","); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(j.f, "  {\"series\": %q, \"timestamp\": %d, \"value\": %g}",
			p.SeriesLabels, p.Timestamp, p.Value); err != nil {
			return err
		}
	}
	return nil
}

func (j *jsonStreamWriter) Close() error {
	if j.first {
		if _, err := fmt.Fprintln(j.f, "[]"); err != nil {
			j.f.Close()
			return err
		}
		return j.f.Close()
	}
	if _, err := fmt.Fprintln(j.f, "\n]"); err != nil {
		j.f.Close()
		return err
	}
	return j.f.Close()
}

type promStreamWriter struct {
	f *os.File
}

func (p *promStreamWriter) WritePoints(points []SeriesPoint) error {
	for _, pt := range points {
		ts := float64(pt.Timestamp) / 1000.0
		if _, err := fmt.Fprintf(p.f, "%s %g %g\n", pt.SeriesLabels, pt.Value, ts); err != nil {
			return err
		}
	}
	return nil
}

func (p *promStreamWriter) Close() error { return p.f.Close() }

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
		err = outputCSV(f, points)
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

func outputCSV(w io.Writer, points []SeriesPoint) error {
	writer := csv.NewWriter(w)
	defer writer.Flush()

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
