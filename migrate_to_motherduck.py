#!/usr/bin/env python3
"""
Migrate local ts-indexer database to MotherDuck
Preserves all completed work and allows resuming on any server
"""

import duckdb
import os
from pathlib import Path

def migrate_to_motherduck(
    local_db_path: str = "ts_indexer.db",
    motherduck_db: str = "ts_indexer",
    motherduck_token: str = None
):
    """
    Migrate local database to MotherDuck with progress preservation
    """
    if not motherduck_token:
        motherduck_token = os.environ.get("MOTHERDUCK_TOKEN")
        if not motherduck_token:
            raise ValueError("MOTHERDUCK_TOKEN environment variable required")
    
    if not Path(local_db_path).exists():
        raise FileNotFoundError(f"Local database not found: {local_db_path}")
    
    print(f"🦆 Migrating {local_db_path} to MotherDuck database: {motherduck_db}")
    
    # Connect to MotherDuck
    motherduck_conn_str = f"md:{motherduck_db}?token={motherduck_token}"
    conn = duckdb.connect(motherduck_conn_str)
    
    # Attach local database
    print("📎 Attaching local database...")
    conn.execute(f"ATTACH '{local_db_path}' AS local_db")
    
    # Create schema in MotherDuck if not exists
    print("🔧 Creating MotherDuck schema...")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS datasets (
            dataset_id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL,
            description TEXT,
            source_bucket VARCHAR,
            source_prefix VARCHAR,
            schema_version VARCHAR,
            dataset_type VARCHAR,
            source VARCHAR,
            storage_location VARCHAR,
            item_id_type VARCHAR,
            target_type VARCHAR,
            tfc_features TEXT,
            total_series BIGINT DEFAULT 0,
            total_records BIGINT DEFAULT 0,
            avg_series_length DOUBLE,
            min_series_length BIGINT,
            max_series_length BIGINT,
            series_id_columns TEXT,
            indexing_status VARCHAR DEFAULT 'pending',
            indexing_started_at TIMESTAMP,
            indexing_completed_at TIMESTAMP,
            indexing_error TEXT,
            metadata_file_path VARCHAR,
            data_file_count BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Copy datasets from local to MotherDuck
    print("📊 Copying dataset records...")
    result = conn.execute("""
        INSERT OR REPLACE INTO datasets 
        SELECT * FROM local_db.datasets
    """)
    
    # Get migration statistics
    local_stats = conn.execute("SELECT COUNT(*) FROM local_db.datasets").fetchone()[0]
    motherduck_stats = conn.execute("SELECT COUNT(*) FROM datasets").fetchone()[0]
    completed_stats = conn.execute("""
        SELECT COUNT(*) FROM datasets WHERE indexing_status = 'completed'
    """).fetchone()[0]
    
    print(f"✅ Migration completed!")
    print(f"📈 Records migrated: {local_stats}")
    print(f"🦆 MotherDuck total: {motherduck_stats}")
    print(f"✅ Completed datasets: {completed_stats}")
    
    # Show progress summary
    progress = conn.execute("""
        SELECT 
            indexing_status,
            COUNT(*) as count,
            SUM(total_series) as series,
            SUM(total_records) as records
        FROM datasets 
        GROUP BY indexing_status
        ORDER BY indexing_status
    """).fetchall()
    
    print("\n📊 Current Progress:")
    print("Status      | Count | Series    | Records")
    print("------------|-------|-----------|------------")
    for status, count, series, records in progress:
        series = series or 0
        records = records or 0
        print(f"{status:11} | {count:5} | {series:9,} | {records:10,}")
    
    # Clean up
    conn.execute("DETACH local_db")
    conn.close()
    
    print(f"\n🎉 Ready to resume indexing on any server!")
    print(f"🚀 Command: ts-indexer index --database 'md:{motherduck_db}'")

if __name__ == "__main__":
    import sys
    
    local_db = sys.argv[1] if len(sys.argv) > 1 else "ts_indexer.db"
    motherduck_db = sys.argv[2] if len(sys.argv) > 2 else "ts_indexer"
    
    migrate_to_motherduck(local_db, motherduck_db)