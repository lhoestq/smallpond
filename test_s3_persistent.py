#!/usr/bin/env python

"""
Test script to verify S3 integration in smallpond with persistent DuckDB storage.

This script demonstrates reading Parquet files from S3 using a persistent DuckDB database
to avoid Out of Memory errors when processing large datasets. It properly configures
temp directories and memory limits for optimal performance.

Usage:
    python test_s3_persistent.py --s3-path s3://your-bucket/data.parquet --region us-west-2
"""

import argparse
import logging
import os
import shutil
import sys
import tempfile
import traceback
from pathlib import Path
from typing import Optional, List, Dict, Any

import duckdb

# Import smallpond
try:
    import smallpond
    from smallpond.common import OutOfMemory, MB, GB
    from smallpond.execution.task import ExecSqlQueryMixin
except ImportError:
    # Will handle this properly in the main function
    pass

# Configuration
MEMORY_LIMIT_GB = 16
TEMP_DIR_SIZE_GB = 20  # Size to reserve for temp storage

def setup_persistent_storage():
    """Set up persistent storage for DuckDB."""
    # Create base directories
    base_dir = Path(tempfile.gettempdir()) / "smallpond_persistent"
    
    # Create subdirectories
    db_dir = base_dir / "db"
    temp_dir = base_dir / "temp"
    
    # Ensure directories exist
    db_dir.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    # Create database file path
    db_file = db_dir / "smallpond.db"
    
    # Check available disk space for temp directory
    try:
        total, used, free = shutil.disk_usage(temp_dir)
        free_gb = free / (1024 ** 3)
        print(f"Available disk space for temp directory: {free_gb:.2f} GB")
        
        if free_gb < TEMP_DIR_SIZE_GB:
            print(f"WARNING: Less than {TEMP_DIR_SIZE_GB} GB available for temp storage")
    except Exception as e:
        print(f"Error checking disk space: {e}")
    
    print(f"Using persistent DuckDB database: {db_file}")
    print(f"Using temp directory: {temp_dir}")
    
    return str(db_file), str(temp_dir)

def initialize_duckdb(db_file, temp_dir):
    """Initialize DuckDB with persistent storage and proper settings."""
    try:
        # Close any existing connection first
        try:
            duckdb.disconnect()
        except:
            pass
        
        # Create a new persistent connection
        conn = duckdb.connect(db_file)
        
        # Configure DuckDB for optimal performance with large datasets
        conn.execute("PRAGMA enable_object_cache")
        conn.execute(f"SET memory_limit='{MEMORY_LIMIT_GB}GB'")
        conn.execute(f"SET temp_directory='{temp_dir}'")
        conn.execute("SET enable_progress_bar=true")
        conn.execute("SET preserve_insertion_order=false")  # Better performance
        conn.execute("SET arrow_large_buffer_size=true")    # Better Arrow integration
        
        print("DuckDB connection initialized with persistent storage")
        return conn
    except Exception as e:
        print(f"Error initializing DuckDB connection: {e}")
        return None

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Test smallpond S3 integration with persistent storage")
    parser.add_argument("--s3-path", type=str, required=True,
                        help="S3 path to Parquet file(s) (s3://bucket/path)")
    parser.add_argument("--region", type=str, default=None,
                        help="AWS region (e.g., us-west-2)")
    parser.add_argument("--access-key", type=str, default=None,
                        help="AWS access key ID")
    parser.add_argument("--secret-key", type=str, default=None,
                        help="AWS secret access key")
    parser.add_argument("--session-token", type=str, default=None,
                        help="AWS session token")
    parser.add_argument("--endpoint", type=str, default=None,
                        help="Custom S3 endpoint for non-AWS S3-compatible storage")
    parser.add_argument("--recursive", action="store_true",
                        help="Recursively search for Parquet files")
    parser.add_argument("--partitions", type=int, default=8,
                        help="Number of partitions for data processing (default: 8)")
    parser.add_argument("--debug", action="store_true",
                        help="Enable debug logging")
    return parser.parse_args()

def check_s3_secrets(conn):
    """Check if any S3 secrets are configured in DuckDB."""
    try:
        result = conn.execute("SELECT * FROM duckdb_secrets();").fetchall()
        print("\nConfigured DuckDB secrets:")
        if result:
            for row in result:
                print(f"  {row}")
        else:
            print("  No secrets found")
        return bool(result)
    except Exception as e:
        print(f"\nError checking DuckDB secrets: {e}")
        return False

def check_duckdb_version() -> Optional[str]:
    """Check if DuckDB version supports Secrets Manager."""
    try:
        version = duckdb.__version__
        print(f"\nDuckDB version: {version}")
        
        # Parse the version string
        major, minor, *_ = version.split('.')
        version_ok = (int(major) > 0) or (int(major) == 0 and int(minor) >= 8)
        
        if not version_ok:
            print("WARNING: DuckDB Secrets Manager requires version 0.8.0 or newer")
        
        return version
    except Exception as e:
        print(f"Error checking DuckDB version: {e}")
        return None

def test_metadata_query(conn, s3_path: str) -> bool:
    """Test if we can query metadata directly from the S3 path."""
    print(f"\nTesting direct metadata query for: {s3_path}")
    try:
        metadata = conn.execute(f"SELECT * FROM parquet_metadata('{s3_path}') LIMIT 1").fetchall()
        print(f"Success! Metadata query returned: {metadata}")
        return True
    except Exception as e:
        print(f"Error querying metadata: {e}")
        return False

def init_smallpond_with_persistent_db(conn, temp_dir):
    """Initialize smallpond with our persistent DuckDB connection."""
    try:
        # Enable temp directory in all ExecSqlQueryMixin instances
        ExecSqlQueryMixin.enable_temp_directory = True
        
        # Set the temp directory path for all tasks
        os.environ["DUCKDB_TEMP_DIRECTORY"] = temp_dir
        
        # Use our persistent connection for smallpond
        sp = smallpond.init(duckdb_connection=conn)
        print("Smallpond initialized with persistent DuckDB connection")
        return sp
    except Exception as e:
        print(f"Error initializing smallpond with persistent DB: {e}")
        # Fall back to default initialization
        print("Falling back to default initialization")
        return smallpond.init()

def main():
    """Main function to run the S3 integration test with persistent storage."""
    args = parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Set up persistent storage
    db_file, temp_dir = setup_persistent_storage()
    
    # Initialize DuckDB
    conn = initialize_duckdb(db_file, temp_dir)
    if not conn:
        print("Failed to initialize DuckDB connection")
        return 1
    
    try:
        # Import smallpond here to avoid affecting import time with logging config
        try:
            import smallpond
            print(f"Smallpond version: {smallpond.__version__}")
        except ImportError:
            print("ERROR: smallpond package not found. Please install it with:")
            print("  pip install -e .")
            return 1
        except Exception as e:
            print(f"ERROR: Failed to import smallpond: {e}")
            return 1
        
        # Check DuckDB version
        check_duckdb_version()
        
        # Initialize smallpond session
        print(f"\nInitializing smallpond session...")
        sp = init_smallpond_with_persistent_db(conn, temp_dir)
        
        print(f"\nReading from S3 path: {args.s3_path}")
        
        # Read Parquet data from S3
        try:
            df = sp.read_parquet(
                args.s3_path,
                recursive=args.recursive,
                s3_region=args.region,
                s3_access_key_id=args.access_key,
                s3_secret_access_key=args.secret_key,
                s3_session_token=args.session_token,
                s3_endpoint=args.endpoint
            )
        except Exception as e:
            print(f"ERROR: Failed to read Parquet data from S3: {e}")
            if args.debug:
                traceback.print_exc()
            return 1
        
        # Check if S3 secrets were created
        has_secrets = check_s3_secrets(conn)
        
        # Test direct metadata query
        test_metadata_query(conn, args.s3_path)
        
        # Get basic information about the data
        print("\nAttempting to fetch data schema...")
        try:
            table = df.to_arrow()
            print("\nData Schema:")
            for field in table.schema:
                print(f"  {field.name}: {field.type}")
        except Exception as e:
            print(f"ERROR: Failed to fetch schema: {e}")
            if args.debug:
                traceback.print_exc()
            return 1
        
        # Get row count
        print("\nCounting rows...")
        try:
            num_rows = df.count()
            print(f"\nTotal rows: {num_rows}")
        except Exception as e:
            print(f"ERROR: Failed to count rows: {e}")
            if args.debug:
                traceback.print_exc()
            return 1
        
        # Show a sample of the data
        print("\nFetching sample data...")
        try:
            limit = min(5, num_rows)
            print(f"\nFirst {limit} rows:")
            sample = df.take(limit)
            for row in sample:
                print(row)
        except Exception as e:
            print(f"ERROR: Failed to fetch sample data: {e}")
            if args.debug:
                traceback.print_exc()
            return 1
        
        # Process data in smaller batches to avoid memory issues
        try:
            print("\nRepartitioning data...")
            # Repartition into multiple partitions to reduce memory pressure
            num_partitions = args.partitions
            print(f"Using {num_partitions} partitions for processing")
            
            df = df.repartition(num_partitions, hash_by="hvfhs_license_num")
            
            # Run SQL aggregation query
            print("\nRunning SQL aggregation query...")
            df2 = sp.partial_sql("""
                SELECT 
                    hvfhs_license_num, 
                    min(trip_distance) as min_trip_distance, 
                    max(trip_distance) as max_trip_distance,
                    count(*) as trip_count
                FROM {0} 
                GROUP BY hvfhs_license_num
            """, df)

            # Show the result of the SQL aggregation query
            print("\nResult of SQL aggregation query:")
            # results = df2.take(10)  # Get up to 10 rows from the result
            # for row in results:
            #     print(row)
                
            # Save results to a file in the temp directory
            result_file = os.path.join(temp_dir, "query_results.parquet")
            print(f"\nSaving results to {result_file}")
            df2.write_parquet(result_file)
            print(f"Results saved successfully")
            
        except Exception as e:
            print(f"\nERROR: Failed to process SQL query: {e}")
            if isinstance(e, (duckdb.OutOfMemoryException, OutOfMemory)) or "OutOfMemory" in str(e):
                print("\nMemory error detected. Try one of the following solutions:")
                print(f"1. Increase the memory_limit in the script (currently {MEMORY_LIMIT_GB}GB)")
                print("2. Use a smaller dataset or reduce operations")
                print("3. Run with 'DUCKDB_NO_THREADS=1' environment variable to reduce memory usage")
                print(f"4. Increase the number of partitions (currently {args.partitions})")
            if args.debug:
                traceback.print_exc()
        
        print("\nS3 integration test completed!")
        return 0
        
    finally:
        # Close the persistent connection when done
        if conn:
            try:
                conn.close()
                print("\nClosed persistent DuckDB connection")
            except Exception as e:
                print(f"Error closing connection: {e}")

if __name__ == "__main__":
    sys.exit(main()) 