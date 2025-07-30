#!/bin/bash

# Script to monitor interactive finder logs in real-time
# Usage: ./check_interactive_logs.sh

LOG_FILE="ts_indexer_interactive.log"

echo "🔍 Monitoring interactive finder logs..."
echo "Log file: $LOG_FILE"
echo "----------------------------------------"

if [ -f "$LOG_FILE" ]; then
    echo "📋 Recent log entries:"
    tail -20 "$LOG_FILE"
    echo "----------------------------------------"
    echo "👁️  Following logs in real-time (Ctrl+C to stop):"
    tail -f "$LOG_FILE"
else
    echo "⚠️  Log file not found. Run 'cargo run -- search' (without query) to start interactive mode."
    echo "The log file will be created when the interactive finder starts."
fi