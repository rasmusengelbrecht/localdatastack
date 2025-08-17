#!/bin/bash
# Simple wrapper script for the main orchestrator

echo "🏗️ Starting Local Data Stack Pipeline..."
uv run python localdatastack/run.py "$@"