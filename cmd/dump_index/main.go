package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/prometheus/tsdb/index"
)

func readIndexFromFile(path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}

func readIndexFromS3(bucket, key, region string) ([]byte, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		return nil, err
	}
	svc := s3.NewFromConfig(cfg)
	resp, err := svc.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

func dumpIndex(w io.Writer, data []byte) error {
	idxr, err := index.NewReader(data)
	if err != nil {
		return err
	}
	defer idxr.Close()

	// Collect all label names across the block and build a mapping
	// of metric name to the set of labels that appear for that metric.
	allLabels := map[string]struct{}{}
	metricLabels := map[string]map[string]struct{}{}

	it := idxr.Postings(index.AllPostingsKey())
	for it.Next() {
		id := it.At()
		lset, err := idxr.Labels(id)
		if err != nil {
			return err
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
		return it.Err()
	}

	// Remove '__name__' from overall label list since it's used as metric
	// name column in the CSV.
	delete(allLabels, "__name__")

	var labels []string
	for l := range allLabels {
		labels = append(labels, l)
	}
	sort.Strings(labels)

	cw := csv.NewWriter(w)

	header := append([]string{"metric_name"}, labels...)
	if err := cw.Write(header); err != nil {
		return err
	}

	var metrics []string
	for m := range metricLabels {
		metrics = append(metrics, m)
	}
	sort.Strings(metrics)

	for _, m := range metrics {
		row := make([]string, 0, len(labels)+1)
		row = append(row, m)
		ml := metricLabels[m]
		for _, l := range labels {
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

func main() {
	blockDir := flag.String("block.dir", "", "Path to block directory")
	s3Bucket := flag.String("s3.bucket", "", "S3 bucket containing block")
	s3Prefix := flag.String("s3.prefix", "", "S3 prefix to block directory")
	blockID := flag.String("block.id", "", "Block ID")
	region := flag.String("s3.region", "us-east-1", "AWS region")
	flag.Parse()

	if *blockID == "" {
		log.Fatal("block.id must be specified")
	}

	var data []byte
	var err error
	if *s3Bucket != "" {
		key := filepath.Join(*s3Prefix, *blockID, "index")
		data, err = readIndexFromS3(*s3Bucket, key, *region)
	} else if *blockDir != "" {
		path := filepath.Join(*blockDir, *blockID, "index")
		data, err = readIndexFromFile(path)
	} else {
		log.Fatal("either block.dir or s3.bucket must be specified")
	}
	if err != nil {
		log.Fatal(err)
	}
	if err := dumpIndex(os.Stdout, data); err != nil {
		log.Fatal(err)
	}
}
