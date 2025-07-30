#!/usr/bin/env python3
"""
Modal deployment for ts-indexer with MotherDuck integration
Leverages Modal's distributed processing and persistent storage
"""

import modal
import os
from pathlib import Path

# Create Modal app
app = modal.App("ts-indexer")

# Define image with Rust and dependencies
image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("curl", "build-essential", "libssl-dev", "pkg-config")
    .run_commands(
        "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y",
        "echo 'source ~/.cargo/env' >> ~/.bashrc",
    )
    .env({"PATH": "/root/.cargo/bin:$PATH"})
    .run_commands(
        # Install DuckDB
        "wget -q https://github.com/duckdb/duckdb/releases/latest/download/libduckdb-linux-amd64.zip",
        "unzip libduckdb-linux-amd64.zip -d /usr/local/",
        "ldconfig",
    )
    .pip_install("duckdb", "boto3", "pydantic")
)

# Persistent volume for database
volume = modal.Volume.from_name("ts-indexer-data", create_if_missing=True)

@app.function(
    image=image,
    volumes={"/data": volume},
    cpu=16,  # High CPU for parallel processing
    memory=32768,  # 32GB memory
    timeout=86400,  # 24 hour timeout
    secrets=[
        modal.Secret.from_name("aws-credentials"),
        modal.Secret.from_name("motherduck-token")  # If using MotherDuck
    ]
)
def run_indexer(
    bucket: str = "tfc-modeling-data-eu-west-3",
    prefix: str = "lotsa_long_format/",
    metadata_prefix: str = "metadata/lotsa_long_format/",
    concurrency: int = 16,
    use_motherduck: bool = True
):
    """
    Run ts-indexer on Modal with persistent storage
    """
    import subprocess
    import sys
    
    # Clone and build ts-indexer (cached in image)
    build_commands = [
        "cd /tmp && git clone https://github.com/your-org/ts-indexer.git || true",
        "cd /tmp/ts-indexer && git pull",
        "cd /tmp/ts-indexer && cargo build --release"
    ]
    
    for cmd in build_commands:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Build error: {result.stderr}")
            return {"error": result.stderr}
    
    # Configure database path
    if use_motherduck:
        # MotherDuck connection string
        db_path = f"md:ts_indexer?token={os.environ['MOTHERDUCK_TOKEN']}"
    else:
        # Local persistent volume
        db_path = "/data/ts_indexer.db"
    
    # Run indexer
    indexer_cmd = [
        "/tmp/ts-indexer/target/release/ts-indexer",
        "index",
        "--bucket", bucket,
        "--prefix", prefix,
        "--metadata-prefix", metadata_prefix,
        "--concurrency", str(concurrency)
    ]
    
    if not use_motherduck:
        indexer_cmd.extend(["--db-path", db_path])
    
    print(f"🚀 Starting indexer with {concurrency} workers...")
    print(f"📊 Database: {'MotherDuck' if use_motherduck else 'Local volume'}")
    
    # Stream output in real-time
    process = subprocess.Popen(
        indexer_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1
    )
    
    output_lines = []
    for line in iter(process.stdout.readline, ''):
        print(line.strip())  # Real-time output
        output_lines.append(line.strip())
    
    process.wait()
    
    # Commit volume changes if using local storage
    if not use_motherduck:
        volume.commit()
    
    return {
        "status": "completed" if process.returncode == 0 else "failed",
        "return_code": process.returncode,
        "output": output_lines[-50:]  # Last 50 lines
    }

@app.function(
    image=image,
    volumes={"/data": volume}
)
def check_progress(use_motherduck: bool = True):
    """
    Check indexing progress without interrupting the main process
    """
    if use_motherduck:
        import duckdb
        conn = duckdb.connect(f"md:ts_indexer?token={os.environ['MOTHERDUCK_TOKEN']}")
    else:
        # Read-only connection to local database
        import duckdb
        conn = duckdb.connect("/data/ts_indexer.db", read_only=True)
    
    try:
        # Get progress statistics
        stats = conn.execute("""
            SELECT 
                COUNT(*) as total_datasets,
                SUM(CASE WHEN indexing_status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN indexing_status = 'in_progress' THEN 1 ELSE 0 END) as in_progress,
                SUM(CASE WHEN indexing_status = 'failed' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN indexing_status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(total_series) as total_series,
                SUM(total_records) as total_records
            FROM datasets
        """).fetchone()
        
        if stats:
            total, completed, in_progress, failed, pending = stats[:5]
            total_series, total_records = stats[5:7]
            
            progress = {
                "total_datasets": total,
                "completed": completed,
                "in_progress": in_progress, 
                "failed": failed,
                "pending": pending,
                "completion_percentage": (completed / total * 100) if total > 0 else 0,
                "total_series": total_series or 0,
                "total_records": total_records or 0
            }
            
            return progress
        else:
            return {"error": "No datasets found"}
            
    except Exception as e:
        return {"error": str(e)}
    finally:
        conn.close()

# CLI functions for easy deployment
@app.local_entrypoint()
def deploy():
    """Deploy indexer to Modal"""
    print("🚀 Deploying ts-indexer to Modal...")
    
    # Start indexing process
    result = run_indexer.remote(
        bucket="tfc-modeling-data-eu-west-3",
        concurrency=16,
        use_motherduck=True  # Change to False for local volume
    )
    
    print("📊 Final result:", result)

@app.local_entrypoint()
def status():
    """Check indexing progress"""
    print("📊 Checking indexing progress...")
    
    progress = check_progress.remote(use_motherduck=True)
    
    if "error" in progress:
        print(f"❌ Error: {progress['error']}")
    else:
        print(f"✅ Progress: {progress['completed']}/{progress['total_datasets']} datasets ({progress['completion_percentage']:.1f}%)")
        print(f"📈 Series: {progress['total_series']:,}")
        print(f"📊 Records: {progress['total_records']:,}")
        print(f"🔄 In Progress: {progress['in_progress']}")
        print(f"❌ Failed: {progress['failed']}")

if __name__ == "__main__":
    # Local development
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        status()
    else:
        deploy()