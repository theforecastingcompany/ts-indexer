#!/usr/bin/env python3
"""
Analyze real dataset metadata and structure for storage optimization recommendations.
Focus on ts-id based access patterns for plotting and statistics.
"""

import boto3
import polars as pl
import pandas as pd
import yaml
import io
from typing import Dict, List, Any
import json

def analyze_metadata_file(s3_client, bucket: str, metadata_key: str) -> Dict[str, Any]:
    """Download and parse a metadata YAML file."""
    print(f"\n🔍 Analyzing metadata: {metadata_key}")
    
    try:
        response = s3_client.get_object(Bucket=bucket, Key=metadata_key)
        metadata_content = response['Body'].read().decode('utf-8')
        metadata = yaml.safe_load(metadata_content)
        
        # Extract key information
        analysis = {
            'name': metadata.get('name', 'unknown'),
            'description': metadata.get('description', ''),
            'ts_id_columns': [],
            'date_columns': [],
            'target_columns': [],
            'covariate_columns': [],
            'metadata': metadata
        }
        
        # Parse ts_id (series identifier columns)
        if 'ts_id' in metadata:
            for ts_field in metadata['ts_id']:
                analysis['ts_id_columns'].append({
                    'name': ts_field.get('name'),
                    'type': ts_field.get('type'),
                    'description': ts_field.get('description', '')
                })
        
        # Parse target columns (can be single dict or list)
        if 'target' in metadata:
            if isinstance(metadata['target'], dict):
                analysis['target_columns'].append({
                    'name': metadata['target'].get('name'),
                    'type': metadata['target'].get('type'),
                    'description': metadata['target'].get('description', '')
                })
            elif isinstance(metadata['target'], list):
                for target in metadata['target']:
                    analysis['target_columns'].append({
                        'name': target.get('name'),
                        'type': target.get('type'),
                        'description': target.get('description', '')
                    })
        
        # Parse targets (plural form)
        if 'targets' in metadata:
            for target in metadata['targets']:
                analysis['target_columns'].append({
                    'name': target.get('name'),
                    'type': target.get('type'),
                    'description': target.get('description', '')
                })
            
        # Parse date columns (can be single dict or list)
        if 'date' in metadata:
            if isinstance(metadata['date'], dict):
                analysis['date_columns'].append({
                    'name': metadata['date'].get('name'),
                    'type': metadata['date'].get('type'),  
                    'format': metadata['date'].get('format', ''),
                    'description': metadata['date'].get('description', '')
                })
            elif isinstance(metadata['date'], list):
                for date_col in metadata['date']:
                    analysis['date_columns'].append({
                        'name': date_col.get('name'),
                        'type': date_col.get('type'),  
                        'format': date_col.get('format', ''),
                        'description': date_col.get('description', '')
                    })
        
        # Note: These datasets appear to use timestamp implicitly, not explicit date columns
            
        # Parse covariates (structured as hist/future)
        if 'covariates' in metadata:
            if isinstance(metadata['covariates'], dict):
                # Handle hist covariates
                if 'hist' in metadata['covariates']:
                    for cov in metadata['covariates']['hist']:
                        analysis['covariate_columns'].append({
                            'name': cov.get('name'),
                            'type': cov.get('type'),
                            'description': cov.get('description', ''),
                            'category': 'historical'
                        })
                # Handle future covariates
                if 'future' in metadata['covariates']:
                    for cov in metadata['covariates']['future']:
                        analysis['covariate_columns'].append({
                            'name': cov.get('name'),
                            'type': cov.get('type'),
                            'description': cov.get('description', ''),
                            'category': 'future'
                        })
            elif isinstance(metadata['covariates'], list):
                # Handle simple list of covariates
                for cov in metadata['covariates']:
                    analysis['covariate_columns'].append({
                        'name': cov.get('name'),
                        'type': cov.get('type'),
                        'description': cov.get('description', ''),
                        'category': 'unknown'
                    })
        
        return analysis
        
    except Exception as e:
        print(f"❌ Error analyzing {metadata_key}: {e}")
        return None

def analyze_parquet_sample(s3_client, bucket: str, data_key: str, ts_id_columns: List[str]) -> Dict[str, Any]:
    """Download and analyze a sample parquet file."""
    print(f"📊 Analyzing parquet sample: {data_key}")
    
    try:
        # Download parquet file
        response = s3_client.get_object(Bucket=bucket, Key=data_key)
        parquet_data = response['Body'].read()
        
        # Read with polars for efficiency
        df = pl.read_parquet(io.BytesIO(parquet_data))
        
        analysis = {
            'file_size_mb': len(parquet_data) / (1024 * 1024),
            'num_rows': len(df),
            'num_columns': len(df.columns),
            'columns': df.columns,
            'dtypes': {col: str(df[col].dtype) for col in df.columns},
            'ts_id_cardinality': {},
            'date_info': {},
            'sample_data': {}
        }
        
        # Analyze ts_id columns for cardinality
        for ts_col in ts_id_columns:
            if ts_col in df.columns:
                unique_count = df[ts_col].n_unique()
                analysis['ts_id_cardinality'][ts_col] = unique_count
                
                # Sample values
                sample_values = df[ts_col].unique().head(10).to_list()
                analysis['sample_data'][ts_col] = sample_values
        
        # Analyze date columns
        date_candidates = [col for col in df.columns if any(keyword in col.lower() 
                          for keyword in ['date', 'time', 'timestamp', 'dt'])]
        
        for date_col in date_candidates:
            analysis['date_info'][date_col] = {
                'dtype': str(df[date_col].dtype),
                'min': str(df[date_col].min()),
                'max': str(df[date_col].max()),
                'sample': df[date_col].head(5).to_list()
            }
        
        # Show sample of first few rows
        print(f"   📋 Columns: {df.columns}")
        print(f"   📈 Shape: {df.shape}")
        print(f"   💾 File size: {analysis['file_size_mb']:.1f} MB")
        
        return analysis
        
    except Exception as e:
        print(f"❌ Error analyzing parquet {data_key}: {e}")
        return None

def main():
    """Analyze the largest datasets for storage optimization."""
    
    # S3 configuration
    bucket = "tfc-modeling-data-eu-west-3"
    region = "eu-west-3"
    
    # Initialize S3 client
    s3_client = boto3.client('s3', region_name=region)
    
    # Target datasets to analyze (diverse sources and sizes)
    datasets_to_analyze = [
        "alibaba_cluster_trace_2018",
        "azure_vm_traces_2017", 
        "borg_cluster_data_2011",
        "australian_electricity_demand",
        "bitcoin_dataset_with_missing_values",
        "electricity_hourly",
        "m3_monthly",
        "m4_daily",
        "traffic_hourly",
        "wiki_rolling_ndays_dataset"
    ]
    
    results = {}
    
    for dataset_name in datasets_to_analyze:
        print(f"\n{'='*60}")
        print(f"🔬 ANALYZING DATASET: {dataset_name}")
        print(f"{'='*60}")
        
        # 1. Analyze metadata
        metadata_key = f"metadata/lotsa_long_format/{dataset_name}_metadata.yaml"
        metadata_analysis = analyze_metadata_file(s3_client, bucket, metadata_key)
        
        if not metadata_analysis:
            continue
            
        # 2. List data files 
        data_prefix = f"lotsa_long_format/{dataset_name}/"
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=data_prefix)
            
            data_files = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Key'].endswith('.parquet'):
                            data_files.append({
                                'key': obj['Key'],
                                'size': obj['Size'],
                                'modified': obj['LastModified']
                            })
            
            print(f"📁 Found {len(data_files)} parquet files")
            
            # Sample 2-3 files for analysis
            sample_files = data_files[:3] if len(data_files) > 3 else data_files
            parquet_analyses = []
            
            ts_id_column_names = [col['name'] for col in metadata_analysis['ts_id_columns']]
            
            for data_file in sample_files:
                parquet_analysis = analyze_parquet_sample(
                    s3_client, bucket, data_file['key'], ts_id_column_names
                )
                if parquet_analysis:
                    parquet_analyses.append(parquet_analysis)
            
            # Store results
            results[dataset_name] = {
                'metadata': metadata_analysis,
                'num_files': len(data_files),
                'total_size_gb': sum(f['size'] for f in data_files) / (1024**3),
                'parquet_samples': parquet_analyses
            }
            
        except Exception as e:
            print(f"❌ Error listing data files for {dataset_name}: {e}")
    
    # Generate analysis report
    print(f"\n{'='*80}")
    print("📋 DATASET ANALYSIS SUMMARY")
    print(f"{'='*80}")
    
    for dataset_name, analysis in results.items():
        metadata = analysis['metadata']
        samples = analysis['parquet_samples']
        
        print(f"\n📊 **{dataset_name.upper()}**")
        print(f"   📝 Description: {metadata['description'][:100]}...")
        print(f"   📁 Files: {analysis['num_files']} parquet files")
        print(f"   💾 Total size: {analysis['total_size_gb']:.2f} GB")
        
        print(f"   🔑 TS-ID Columns:")
        for ts_col in metadata['ts_id_columns']:
            print(f"      • {ts_col['name']} ({ts_col['type']})")
            
        print(f"   📅 Date Columns:")
        for date_col in metadata['date_columns']:
            print(f"      • {date_col['name']} ({date_col['type']}) - {date_col.get('format', 'no format')}")
            
        print(f"   🎯 Target Columns:")
        for target_col in metadata['target_columns']:
            print(f"      • {target_col['name']} ({target_col['type']})")
        
        if samples:
            sample = samples[0]  # First sample
            print(f"   📈 Sample File Analysis:")
            print(f"      • Rows per file: ~{sample['num_rows']:,}")
            print(f"      • File size: ~{sample['file_size_mb']:.1f} MB")
            print(f"      • Columns: {len(sample['columns'])}")
            
            print(f"   🔍 TS-ID Cardinality (per file):")
            for ts_col, cardinality in sample['ts_id_cardinality'].items():
                print(f"      • {ts_col}: {cardinality:,} unique values")
                if ts_col in sample['sample_data']:
                    sample_vals = sample['sample_data'][ts_col][:3]
                    print(f"         Examples: {sample_vals}")
            
            print(f"   ⏰ Date Column Analysis:")
            for date_col, date_info in sample['date_info'].items():
                print(f"      • {date_col}: {date_info['dtype']}")
                print(f"         Range: {date_info['min']} to {date_info['max']}")
    
    # Generate storage recommendations
    print(f"\n{'='*80}")
    print("💡 STORAGE OPTIMIZATION RECOMMENDATIONS")
    print(f"{'='*80}")
    
    for dataset_name, analysis in results.items():
        metadata = analysis['metadata']
        samples = analysis['parquet_samples']
        
        print(f"\n🎯 **{dataset_name}**")
        
        # Analyze ts-id based partitioning potential
        if samples and samples[0]['ts_id_cardinality']:
            sample = samples[0]
            total_files = analysis['num_files']
            
            print(f"   📊 Current Structure: {total_files} files, ~{sample['num_rows']:,} rows/file")
            
            # Calculate ts-id cardinality across dataset
            for ts_col, per_file_cardinality in sample['ts_id_cardinality'].items():
                estimated_total_series = per_file_cardinality * total_files * 0.8  # Rough estimate
                
                print(f"   🔑 {ts_col} Analysis:")
                print(f"      • ~{per_file_cardinality:,} unique values per file")
                print(f"      • Estimated ~{estimated_total_series:,} total unique series")
                
                # Partitioning recommendation
                if estimated_total_series < 100:
                    rec = "Single partition - too few series"
                elif estimated_total_series < 1000:
                    rec = f"Partition by {ts_col} directly (manageable cardinality)"
                elif estimated_total_series < 10000:
                    rec = f"Partition by {ts_col} with grouping (hash into 10-50 buckets)"
                else:
                    rec = f"Partition by {ts_col} with heavy grouping (hash into 100+ buckets)"
                
                print(f"      • 💡 Recommendation: {rec}")
        
        # Date-based partitioning analysis
        if metadata['date_columns'] and samples:
            date_col = metadata['date_columns'][0]['name']
            if date_col in samples[0]['date_info']:
                date_info = samples[0]['date_info'][date_col]
                print(f"   📅 Date-based partitioning for '{date_col}':")
                print(f"      • Range: {date_info['min']} to {date_info['max']}")
                print(f"      • 💡 Consider: year/month partitions for time-range queries")
        
        # Final recommendation
        print(f"   🏆 OPTIMAL STRATEGY for ts-id access:")
        if samples and samples[0]['ts_id_cardinality']:
            ts_cols = list(samples[0]['ts_id_cardinality'].keys())
            primary_ts_col = ts_cols[0] if ts_cols else "ts_id"  
            
            print(f"      1. Primary partition by {primary_ts_col} (hash-based grouping)")
            print(f"      2. Secondary partition by date (year/month) for time filtering")
            print(f"      3. Target: ~1000-5000 series per partition file")
            print(f"      4. Benefits: Direct ts-id access for plotting & statistics")
    
    # Save detailed results
    with open('dataset_analysis_results.json', 'w') as f:
        # Convert datetime objects to strings for JSON serialization
        import json
        def default_serializer(obj):
            if hasattr(obj, 'isoformat'):
                return obj.isoformat()
            raise TypeError(f"Object {obj} is not JSON serializable")
        
        json.dump(results, f, indent=2, default=default_serializer)
    
    print(f"\n💾 Detailed results saved to: dataset_analysis_results.json")

if __name__ == "__main__":
    main()