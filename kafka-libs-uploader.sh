#!/bin/bash

# Uploads all kafka and connect jars in the current directory to S3
# Usage: ./kafka-libs-uploader.sh
# Script needs to be run in the top level directory of the kafka folder once built
VERSION=3.2.6-tl
S3_BASE_LOCATION=s3://ops.triplelift.net/public/kafka/${VERSION}/
for jar in $(find . -name "*.jar"); do aws s3 cp $jar s3://ops.triplelift.net/public/kafka/${VERSION}/$(basename $jar); done
