#!/bin/bash

VERSION="0.1.0"

# build in the specific pipelines
PIPELINE_DIR="pipelines"
PIPELINE_PREFIX="file:///app"
PIPELINES_URLS=""

# Retrieve all the sub files and build the URL list
for file in "$PIPELINE_DIR"/*; do
    if [[ -f "$file" ]] && [[ "$file" == *.py ]]; then
        if [ -z "$PIPELINES_URLS" ]; then
            PIPELINES_URLS="$PIPELINE_PREFIX/$file"
        else
            PIPELINES_URLS="$PIPELINES_URLS;$PIPELINE_PREFIX/$file"
        fi
    fi
done

echo "New Custom Install Pipes: $PIPELINES_URLS"

# Use the variable directly in the build command
docker build --no-cache -t "sstcaiteam/open-webui-pipelines:$VERSION" --build-arg MINIMUM_BUILD=true --build-arg USE_CUDA=false --build-arg PIPELINES_URLS="$PIPELINES_URLS" -f Dockerfile .
