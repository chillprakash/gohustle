#!/bin/bash
# Script to update import paths in Go files

cd backend

# Find all Go files and update import paths
find . -name "*.go" -type f -exec sed -i '' 's#"gohustle/#"gohustle/backend/#g' {} \;
