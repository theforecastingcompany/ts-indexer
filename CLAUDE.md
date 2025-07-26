# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a time-series indexer for The Forecasting Company (TFC), designed to make 3-10 TB of time-series data in S3 blazingly fast to search and retrieve. The system enables fuzzy search by theme, time-series ID, dataset name, and provides rapid visualization capabilities.

## Data Sources

- **Primary Data**: S3 bucket `tfc-modeling-data-eu-west-3` at prefix `lotsa_long_format/`
- **Metadata**: S3 bucket `tfc-modeling-data-eu-west-3` at prefix `metadata/lotsa_long_format/`
- **Schema Reference**: `theforecastingcompany/navi` repository at `data/training_datasets/src/metadata/common.py`

## Technology Stack

- **Database**: DuckDB for high-performance analytics
- **Frontend**: React with pnpm package manager, hosted on Vercel
- **Visualization**: ECharts for plotting time-series
- **Backend**: Modal for Python services when needed
- **Interface**: CLI initially, with future web interface and Claude-powered chat

## Architecture Goals

The system is being built in phases:
1. **Current Phase**: Backend search and retrieval system with CLI interface
2. **Future Phase**: React web frontend with fast plotting capabilities
3. **Advanced Phase**: Claude-powered chat interface for natural language queries

## Data Format

- Data is stored in long format with multiple time-series per dataset
- Each dataset contains searchable metadata for themes, IDs, and names
- Goal is sub-second retrieval and plotting of individual time-series

## Development Commands

This is a greenfield project - no existing build/test commands are configured yet. Use standard patterns:

```bash
# Frontend (React with pnpm)
pnpm init
pnpm install
pnpm dev

# Python backend components
uv init
uv add duckdb
```

## Key Design Principles

- **Performance First**: Sub-second search and visualization is the primary requirement
- **Scalability**: Must handle 3-10 TB of time-series data efficiently  
- **User Experience**: Enable rapid exploration of massive time-series datasets
- **Extensibility**: Built to support future chat interface integration