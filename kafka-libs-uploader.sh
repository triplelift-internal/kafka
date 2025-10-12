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
VERSION=3.6.2-tl
S3_BASE_LOCATION=s3://ops.triplelift.net/public/kafka/${VERSION}

# Create a temporary directory for unique jars
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT

# Find all jars and only copy unique ones based on filename
for jar in $(find . -name "*.jar")
do
    jar_name=$(basename "$jar")
    if [ ! -f "${TEMP_DIR}/${jar_name}" ]; then
        cp "$jar" "${TEMP_DIR}/${jar_name}"
        aws s3 cp "$jar" "${S3_BASE_LOCATION}/${jar_name}"
        echo "Uploaded: $jar_name"
    else
        echo "Skipped duplicate: $jar_name (already uploaded from different path)"
    fi
done
