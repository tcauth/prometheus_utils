# prometheus_utils

## dump_index CLI

`dump_index` reads a Prometheus TSDB block index either from a local
directory or directly from an S3 bucket and writes a CSV listing all
metrics and which labels they use.

### Build

```bash
go build ./cmd/dump_index
```

### Usage

```bash
./dump_index --block.dir /path/to/blocks --block.id <block-id>
./dump_index --s3.bucket <bucket> --s3.prefix <prefix> --block.id <block-id>
```

By default the results are written under `<working-dir>/<bucket>/<tenant>/<block>/<metric-name>/<output-filename>.<ext>`. Use `-ouput-filename` to set the file name explicitly. If not provided a random 4 digit number is used.

Use `-chunk-file-workers` to control how many chunk files are processed in parallel. The existing `-chunk-workers` flag controls parallelism within each file.
