#!/bin/bash
# Script to fix all import paths in Go files

cd backend

# Find all Go files and fix the duplicated backend in import paths
find . -name "*.go" -type f -exec sed -i '' 's#"gohustle/backend/backend/#"gohustle/backend/#g' {} \;
