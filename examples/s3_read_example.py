#!/usr/bin/env python

"""
Example script demonstrating how to read Parquet files from S3 using smallpond.

Before running this script, make sure:
1. You have AWS credentials configured (either via environment variables, ~/.aws/credentials, or IAM role)
2. You have appropriate access to the S3 bucket you're trying to read from
3. Replace the example S3 path with your own path to a Parquet file or folder

This example uses DuckDB's Secrets Manager under the hood to securely handle S3 credentials.

You can run this script with:
    python s3_read_example.py --s3-path s3://your-bucket/data.parquet --region us-west-2
"""

import os
import argparse
import smallpond


def parse_args():
    parser = argparse.ArgumentParser(description="Read Parquet files from S3 using smallpond")
    parser.add_argument("--s3-path", type=str, default="s3://your-bucket/path/to/data.parquet",
                        help="S3 path to Parquet file(s)")
    parser.add_argument("--region", type=str, default=None,
                        help="AWS region (e.g., us-west-2)")
    parser.add_argument("--access-key", type=str, default=None,
                        help="AWS access key ID")
    parser.add_argument("--secret-key", type=str, default=None,
                        help="AWS secret access key")
    parser.add_argument("--endpoint", type=str, default=None,
                        help="Custom S3 endpoint for non-AWS S3-compatible storage")
    parser.add_argument("--recursive", action="store_true",
                        help="Recursively search for Parquet files")
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Initialize smallpond session
    sp = smallpond.init()
    
    print(f"Reading Parquet data from: {args.s3_path}")
    
    # Read Parquet data from S3
    df = sp.read_parquet(
        args.s3_path,
        recursive=args.recursive,
        s3_region=args.region,
        s3_access_key_id=args.access_key,
        s3_secret_access_key=args.secret_key,
        s3_endpoint=args.endpoint
    )
    
    # Get basic information about the data
    # First, let's check the schema
    print("\nData Schema:")
    table = df.to_arrow()
    for field in table.schema:
        print(f"  {field.name}: {field.type}")
    
    # Get number of rows and show first few rows
    num_rows = df.count()
    print(f"\nTotal rows: {num_rows}")
    
    # Show a sample of the data
    limit = min(5, num_rows)
    print(f"\nFirst {limit} rows:")
    sample = df.take(limit)
    for row in sample:
        print(row)


if __name__ == "__main__":
    main() 