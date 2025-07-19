#!/bin/bash
set -euo pipefail

usage() {
  echo "Usage: $0 <project-id> <region> <bucket> <dataset> <api-token> [trust-store-secret-id] [trust-store-secret-version]" >&2
  exit 1
}

if [ "$#" -ne 5 ] && [ "$#" -ne 7 ]; then
  usage
fi

if ! command -v mvn >/dev/null 2>&1; then
  echo "mvn command not found. Please install Maven." >&2
  exit 1
fi

PROJECT_ID=$1
REGION=$2
BUCKET=$3
DATASET=$4
API_TOKEN=$5
if [ "$#" -eq 7 ]; then
  TRUST_STORE_SECRET_ID=$6
  TRUST_STORE_SECRET_VERSION=$7
else
  TRUST_STORE_SECRET_ID="cwa-trust-pem"
  TRUST_STORE_SECRET_VERSION="latest"
fi

mvn compile exec:java \
  -Dexec.mainClass=com.example.WeatherPipeline \
  -Dexec.args="--runner=DataflowRunner \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --inputTopic=projects/${PROJECT_ID}/topics/weather_stn_id \
    --outputPath=gs://${BUCKET}/weather/output \
    --bigQueryTable=${PROJECT_ID}:${DATASET}.weather_raw \
    --apiToken=${API_TOKEN} \
    --trustStoreSecretId=${TRUST_STORE_SECRET_ID} \
    --trustStoreSecretVersion=${TRUST_STORE_SECRET_VERSION}"

