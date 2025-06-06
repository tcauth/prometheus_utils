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

The generated CSV is written to standard output.
