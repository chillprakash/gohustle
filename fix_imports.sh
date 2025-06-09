#!/bin/bash
# Script to fix import paths in Go files

cd backend

# Find all Go files and update import paths
find . -name "*.go" -type f -exec sed -i '' 's#"gohustle/backend/#"gohustle/backend/#g' {} \;
find . -name "*.go" -type f -exec sed -i '' 's#"gohustle/#"gohustle/backend/#g' {} \;
