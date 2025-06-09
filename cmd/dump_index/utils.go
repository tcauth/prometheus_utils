package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func getBucketRegion(s3Client *s3.Client, bucket string, ctx context.Context) (string, error) {
	locationResp, err := s3Client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get bucket location: %w", err)
	}
	if locationResp.LocationConstraint == "" {
		return "us-east-1", nil
	}
	return string(locationResp.LocationConstraint), nil
}

func parseBlockPath(blockPath string) (bucket, tenant, blockID string, err error) {
	if !strings.HasPrefix(blockPath, "s3://") {
		return "", "", "", fmt.Errorf("block path must start with s3://")
	}
	path := strings.TrimPrefix(blockPath, "s3://")
	parts := strings.SplitN(path, "/", 3)
	if len(parts) < 3 {
		return "", "", "", fmt.Errorf("block path must be in format s3://bucket/tenant/block-id")
	}
	return parts[0], parts[1], parts[2], nil
}
