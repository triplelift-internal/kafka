# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

# Uploads all kafka and connect jars in the current directory to S3
# Usage: ./kafka-libs-uploader.sh
# Script needs to be run in the top level directory of the kafka folder once built
VERSION=3.6.2-tl-test-13
S3_BASE_LOCATION=s3://ops.triplelift.net/public/kafka/${VERSION}

# Create a temporary directory for unique jars
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT

# Find all jars and only copy unique ones based on filename
for jar in $(find . -name "*.jar")
do
    jar_name=$(basename "$jar")
    if [ ! -f "${TEMP_DIR}/${jar_name}" ]; then
        cp -v "$jar" "${TEMP_DIR}/${jar_name}"
    fi
done

# Count total number of files to upload
TOTAL_FILES=$(find "$TEMP_DIR" -type f | wc -l)
echo "Uploading $TOTAL_FILES files to ${S3_BASE_LOCATION}..."

# Upload function for parallel execution
upload_file() {
    local file="$1"
    local s3_location="$2"
    echo "Uploading $(basename "$file")"
    aws s3 cp "$file" "$s3_location/$(basename "$file")"
}

export -f upload_file

# Use parallel uploads with background processes
max_parallel=50
count=0
for file in "$TEMP_DIR"/*.jar; do
    upload_file "$file" "$S3_BASE_LOCATION" &
    ((count++))
    if ((count % max_parallel == 0)); then
        wait
    fi
done
wait

echo "Upload complete!"
