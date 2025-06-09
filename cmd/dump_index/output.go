package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
)

func outputResults(points []SeriesPoint, cfg Config) error {
	switch cfg.OutputFormat {
	case "csv":
		return outputCSV(points)
	case "json":
		return outputJSON(points)
	case "prometheus":
		return outputPrometheus(points)
	default:
		return fmt.Errorf("unsupported output format: %s", cfg.OutputFormat)
	}
}

func outputCSV(points []SeriesPoint) error {
	writer := csv.NewWriter(os.Stdout)
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

func outputJSON(points []SeriesPoint) error {
	fmt.Println("[")
	for i, point := range points {
		if i > 0 {
			fmt.Println(",")
		}
		fmt.Printf(`  {"series": %q, "timestamp": %d, "value": %g}`,
			point.SeriesLabels, point.Timestamp, point.Value)
	}
	fmt.Println("\n]")
	return nil
}

func outputPrometheus(points []SeriesPoint) error {
	for _, point := range points {
		// Convert timestamp from milliseconds to seconds for Prometheus format
		timestampSec := float64(point.Timestamp) / 1000.0
		fmt.Printf("%s %g %g\n", point.SeriesLabels, point.Value, timestampSec)
	}
	return nil
}
