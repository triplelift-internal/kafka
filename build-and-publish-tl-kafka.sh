# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


#!/bin/bash

set -e  # Exit on any error

# Set the local path to the Kafka repository
KAFKA_REPO_LOCAL_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check if Kafka repository exists
if [ ! -d "$KAFKA_REPO_LOCAL_PATH" ]; then
    echo "Error: Kafka repository not found at ${KAFKA_REPO_LOCAL_PATH}"
    exit 1
fi

# Extract Kafka version from gradle.properties
export KAFKA_VERSION=$(grep "^version=" ${KAFKA_REPO_LOCAL_PATH}/gradle.properties | cut -d'=' -f2)
if [ -z "$KAFKA_VERSION" ]; then
    echo "Error: KAFKA_VERSION not found in ${KAFKA_REPO_LOCAL_PATH}/gradle.properties"
    exit 1
fi

export S3_BASE_LOCATION=s3://ops.triplelift.net/public/kafka/${KAFKA_VERSION}

# Main execution
main() {
    echo "=========================================="
    echo "Kafka Build and Publish Libs Script"
    echo "=========================================="
    echo ""
    
    if [ "$1" == "build" ]; then
        buildKafka
    elif [ "$1" == "publish" ]; then
        publish
    else
        buildKafka
        publish
    fi
    echo ""
    echo "=========================================="
    echo "All operations completed successfully!"
    echo "=========================================="
}


publish() {
    unset AWS_ACCESS_KEY_ID
    unset AWS_SECRET_ACCESS_KEY
    unset AWS_SESSION_TOKEN
    $(aws configure export-credentials --format env)
    publishKafkaDependencies
}

publishKafkaDependencies() {
    echo "Cleaning S3 location ${S3_BASE_LOCATION} before upload..."
    aws s3 rm ${S3_BASE_LOCATION} --recursive > /dev/null 2>&1
    uploadKafka
}

buildKafka() {
    echo "Building Kafka from ${KAFKA_REPO_LOCAL_PATH}..."
    cd "$KAFKA_REPO_LOCAL_PATH" || { echo "Error: Cannot change to Kafka directory"; exit 1; }
    echo "Kafka Version: $KAFKA_VERSION"
    ./gradlew clean build -x test
    if [ $? -ne 0 ]; then
        echo "Error: Kafka build failed!"
        exit 1
    fi
    ./gradlew :connect:runtime:test
    if [ $? -ne 0 ]; then
        echo "Error: Kafka build failed!"
        exit 1
    fi
    
    echo "Kafka build completed successfully"
    cd - > /dev/null || exit 1
}

# Upload function for parallel execution - defined globally so error handling works
upload_file() {
    local file="$1"
    local s3_location="$2"
    local success_file="$3"
    local error_file="$4"
    
    output=$(aws s3 cp "$file" "$s3_location/$(basename "$file")" 2>&1)
    if [ $? -ne 0 ]; then
        echo "Failed to upload $(basename "$file"): $output" >> "$error_file"
        return 1
    else
        echo "$(basename "$file")" >> "$success_file"
        return 0
    fi
}

export -f upload_file

uploadKafka() {
    echo "Preparing to upload Kafka jars..."
    cd "$KAFKA_REPO_LOCAL_PATH" || { echo "Error: Cannot change to Kafka directory"; exit 1; }
    echo "Uploading Kafka Version: $KAFKA_VERSION to S3 location: $S3_BASE_LOCATION"

    # Create a temporary directory for unique jars
    TEMP_DIR=$(mktemp -d)
    SUCCESS_LOG=$(mktemp)
    ERROR_LOG=$(mktemp)
    trap 'rm -rf "$TEMP_DIR" "$SUCCESS_LOG" "$ERROR_LOG"' EXIT

    # Find all jars and only copy unique ones based on filename
    echo "Collecting unique jar files..."
    for jar in $(find . -name "*.jar")
    do
        jar_name=$(basename "$jar")
        if [ ! -f "${TEMP_DIR}/${jar_name}" ]; then
            cp "$jar" "${TEMP_DIR}/${jar_name}"
        fi
    done

    # Count total number of files to upload
    TOTAL_FILES=$(find "$TEMP_DIR" -type f | wc -l | tr -d ' ')
    
    if [ "$TOTAL_FILES" -eq 0 ]; then
        echo "Error: No jar files found to upload!"
        exit 1
    fi
    
    echo "Found $TOTAL_FILES unique jar files to upload to ${S3_BASE_LOCATION}/..."

    # Use parallel uploads with background processes
    max_parallel=100
    count=0
    uploaded=0
    
    for file in "$TEMP_DIR"/*.jar; do
        upload_file "$file" "$S3_BASE_LOCATION" "$SUCCESS_LOG" "$ERROR_LOG" &
        ((count++))
        ((uploaded++))
        echo "Uploading: $uploaded/$TOTAL_FILES - $(basename "$file")"
        if ((count % max_parallel == 0)); then
            wait
        fi
    done
    wait

    # Check for failures
    success_count=$(wc -l < "$SUCCESS_LOG" | tr -d ' ')
    error_count=$(wc -l < "$ERROR_LOG" | tr -d ' ')
    
    echo ""
    echo "=========================================="
    echo "Upload to ${S3_BASE_LOCATION} Completed:"
    echo "  Total files: $TOTAL_FILES"
    echo "  Successful: $success_count"
    echo "  Failed: $error_count"
    echo "=========================================="
    
    # If no files were successfully uploaded OR there are errors, something went wrong
    if [ "$success_count" -eq 0 ] || [ "$error_count" -gt 0 ]; then
        echo ""
        echo "ERROR: Upload failed!"
        echo ""
        if [ -s "$ERROR_LOG" ]; then
            echo "Error details:"
            cat "$ERROR_LOG"
        else
            echo "No error details captured. This may indicate:"
            echo "  - AWS credentials have expired"
            echo "  - Network connectivity issues"
            echo "  - Permission issues with S3 bucket"
            echo ""
            echo "Please check your AWS credentials and try again."
        fi
        cd - > /dev/null || exit 1
        exit 1
    fi
    
    echo ""
    echo "Upload complete! Successfully uploaded $success_count/$TOTAL_FILES files to ${S3_BASE_LOCATION}"
    cd - > /dev/null || exit 1
}

# Run main function
main "$@"
