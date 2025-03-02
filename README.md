# smallpond

[![CI](https://github.com/deepseek-ai/smallpond/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/deepseek-ai/smallpond/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/smallpond)](https://pypi.org/project/smallpond/)
[![Docs](https://img.shields.io/badge/docs-latest-brightgreen.svg)](https://deepseek-ai.github.io/smallpond/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A lightweight data processing framework built on [DuckDB] and [3FS].

## Features

- 🚀 High-performance data processing powered by DuckDB
- 🌍 Scalable to handle PB-scale datasets
- 🛠️ Easy operations with no long-running services
- 🪣 Direct reading from S3 storage

## Installation

Python 3.8 to 3.12 is supported.

```bash
pip install smallpond
```

## Quick Start

```bash
# Download example data
wget https://duckdb.org/data/prices.parquet
```

```python
import smallpond

# Initialize session
sp = smallpond.init()

# Load data
df = sp.read_parquet("prices.parquet")

# Process data
df = df.repartition(3, hash_by="ticker")
df = sp.partial_sql("SELECT ticker, min(price), max(price) FROM {0} GROUP BY ticker", df)

# Save results
df.write_parquet("output/")
# Show results
print(df.to_pandas())
```

### Reading from S3

```python
import smallpond

# Initialize session
sp = smallpond.init()

# Load data directly from S3
df = sp.read_parquet("s3://my-bucket/data/prices.parquet", 
                     s3_region="us-west-2")

# You can also provide explicit credentials
df = sp.read_parquet("s3://my-bucket/data/*.parquet",
                     recursive=True,
                     s3_region="us-west-2",
                     s3_access_key_id="YOUR_ACCESS_KEY",
                     s3_secret_access_key="YOUR_SECRET_KEY")

# For S3-compatible storage (like MinIO, Ceph, etc.)
df = sp.read_parquet("s3://my-bucket/data.parquet",
                     s3_endpoint="https://minio.example.com",
                     s3_region="us-east-1",
                     s3_access_key_id="YOUR_ACCESS_KEY",
                     s3_secret_access_key="YOUR_SECRET_KEY")

# Process data as usual
df = sp.partial_sql("SELECT * FROM {0} LIMIT 10", df)
print(df.to_pandas())
```

smallpond uses DuckDB's Secrets Manager under the hood to securely handle S3 credentials across the entire session.

## Documentation

For detailed guides and API reference:
- [Getting Started](docs/source/getstarted.rst)
- [API Reference](docs/source/api.rst)

## Performance

We evaluated smallpond using the [GraySort benchmark] ([script]) on a cluster comprising 50 compute nodes and 25 storage nodes running [3FS].  The benchmark sorted 110.5TiB of data in 30 minutes and 14 seconds, achieving an average throughput of 3.66TiB/min.

Details can be found in [3FS - Gray Sort].

[DuckDB]: https://duckdb.org/
[3FS]: https://github.com/deepseek-ai/3FS
[GraySort benchmark]: https://sortbenchmark.org/
[script]: benchmarks/gray_sort_benchmark.py
[3FS - Gray Sort]: https://github.com/deepseek-ai/3FS?tab=readme-ov-file#2-graysort

## Development

```bash
pip install .[dev]

# run unit tests
pytest -v tests/test*.py

# build documentation
pip install .[docs]
cd docs
make html
python -m http.server --directory build/html
```

## License

This project is licensed under the [MIT License](LICENSE).
