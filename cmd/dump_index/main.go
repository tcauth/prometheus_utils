package main

import (
	"context"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/example/prometheus_utils/metricsindex"
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
	idx, err := metricsindex.Build(data)
	if err != nil {
		log.Fatal(err)
	}
	if err := metricsindex.WriteCSV(os.Stdout, idx); err != nil {
		log.Fatal(err)
	}
}
