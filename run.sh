#!/bin/bash
set -euo pipefail

usage() {
  echo "Usage: $0 <project-id> <region> <bucket> <dataset> <api-token> <retry-attempts> [trust-store-secret-id] [trust-store-secret-version]" >&2
  echo "Optional settings can be provided via environment variables:" >&2
  echo "  API_BASE_URL - CWA API endpoint" >&2
  echo "  API_DEFAULT_PARAM - default query parameters" >&2
  echo "  API_HEADERS - additional HTTP headers" >&2
  echo "  TOKEN_IN_HEADER - set to true to send token in header" >&2
  echo "  TOKEN_HEADER_NAME - header name when TOKEN_IN_HEADER=true" >&2
  echo "  INPUT_TOPIC - Pub/Sub topic to read station IDs from" >&2
  echo "  JOB_NAME - Dataflow job name" >&2
  exit 1
}

if [ "$#" -ne 6 ] && [ "$#" -ne 8 ]; then
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
RETRY_ATTEMPTS=$6
if [ "$#" -eq 8 ]; then
  TRUST_STORE_SECRET_ID=$7
  TRUST_STORE_SECRET_VERSION=$8
else
  TRUST_STORE_SECRET_ID="cwa-trust-pem"
  TRUST_STORE_SECRET_VERSION="latest"
fi

# Additional API configuration via environment variables
API_BASE_URL="${API_BASE_URL:-https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0001-001}"
API_DEFAULT_PARAM="${API_DEFAULT_PARAM:-}"
API_HEADERS="${API_HEADERS:-}"
TOKEN_IN_HEADER="${TOKEN_IN_HEADER:-false}"
TOKEN_HEADER_NAME="${TOKEN_HEADER_NAME:-Authorization}"
INPUT_TOPIC="${INPUT_TOPIC:-projects/${PROJECT_ID}/topics/weather_stn_id}"
JOB_NAME="${JOB_NAME:-}"

ARGS="--runner=DataflowRunner \
  --project=${PROJECT_ID} \
  --region=${REGION} \
  --inputTopic=${INPUT_TOPIC} \
  --outputPath=gs://${BUCKET}/weather/output \
  --bigQueryTable=${PROJECT_ID}:${DATASET}.weather_raw \
  --apiToken=${API_TOKEN} \
  --retryAttempts=${RETRY_ATTEMPTS} \
  --trustStoreSecretId=${TRUST_STORE_SECRET_ID} \
  --trustStoreSecretVersion=${TRUST_STORE_SECRET_VERSION} \
  --apiBaseUrl=${API_BASE_URL}"

if [ -n "$API_DEFAULT_PARAM" ]; then
  ARGS+=" --apiDefaultParam=${API_DEFAULT_PARAM}"
fi
if [ -n "$API_HEADERS" ]; then
  ARGS+=" --apiHeaders=${API_HEADERS}"
fi
if [ -n "$JOB_NAME" ]; then
  ARGS+=" --jobName=${JOB_NAME}"
fi
ARGS+=" --tokenInHeader=${TOKEN_IN_HEADER}"
ARGS+=" --tokenHeaderName=${TOKEN_HEADER_NAME}"

mvn compile exec:java \
  -Dexec.mainClass=com.example.WeatherPipeline \
  -Dexec.args="${ARGS}"

