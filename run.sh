#!/bin/bash
set -euo pipefail

if [ "$#" -ne 5 ]; then
  echo "Usage: $0 <project-id> <region> <bucket> <dataset> <api-token>" >&2
  exit 1
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

mvn compile exec:java \
  -Dexec.mainClass=com.example.WeatherPipeline \
  -Dexec.args="--runner=DataflowRunner \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --inputTopic=projects/${PROJECT_ID}/topics/weather_stn_id \
    --outputPath=gs://${BUCKET}/weather/output \
    --bigQueryTable=${PROJECT_ID}:${DATASET}.weather_raw \
    --apiToken=${API_TOKEN}"

