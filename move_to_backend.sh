#!/bin/bash
# Script to move Go files and directories to the backend folder

# Move all directories except frontend, backend, and .git
for dir in $(find . -maxdepth 1 -type d | grep -v "^\.$" | grep -v "^\.\/\." | grep -v "^\.\/frontend$" | grep -v "^\.\/backend$"); do
  mv "$dir" backend/
done

# Move Go files in the root directory
find . -maxdepth 1 -name "*.go" -exec mv {} backend/ \;

# Move go.mod and go.sum
if [ -f go.mod ]; then
  mv go.mod backend/
fi

if [ -f go.sum ]; then
  mv go.sum backend/
fi
