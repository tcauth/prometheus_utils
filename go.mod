module github.com/example/prometheus_utils

go 1.20

require (
    github.com/aws/aws-sdk-go-v2/config v1.29.14
    github.com/aws/aws-sdk-go-v2/service/s3 v1.34.1
    github.com/prometheus/prometheus v0.304.1
)

replace gopkg.in/yaml.v2 => gopkg.in/yaml.v2 v2.4.0

