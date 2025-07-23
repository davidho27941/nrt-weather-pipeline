#!/bin/bash

set -euo pipefail

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <project-id> <region> <bucket> <dataset>" >&2
  exit 1
fi

PROJECT_ID=$1
REGION=$2
BUCKET=$3
DATASET=$4

echo "Configuring project $PROJECT_ID"
gcloud config set project "$PROJECT_ID" >&2

# Create Pub/Sub topic
if ! gcloud pubsub topics describe weather_stn_id >/dev/null 2>&1; then
  gcloud pubsub topics create weather_stn_id
else
  echo "Pub/Sub topic weather_stn_id already exists" >&2
fi

# Create retry Pub/Sub topic
if ! gcloud pubsub topics describe weather_retry >/dev/null 2>&1; then
  gcloud pubsub topics create weather_retry
else
  echo "Pub/Sub topic weather_retry already exists" >&2
fi

# Create GCS bucket
if ! gsutil ls -b "gs://$BUCKET" >/dev/null 2>&1; then
  gsutil mb -p "$PROJECT_ID" -l "$REGION" "gs://$BUCKET"
else
  echo "Bucket gs://$BUCKET already exists" >&2
fi

# Create BigQuery dataset
if ! bq show "$DATASET" >/dev/null 2>&1; then
  bq --location="$REGION" mk -d "$DATASET"
else
  echo "Dataset $DATASET already exists" >&2
fi

# Create BigQuery table
if ! bq show "$DATASET.weather_raw" >/dev/null 2>&1; then
  bq mk --table "$DATASET.weather_raw" "raw_json:STRING"
else
  echo "Table $DATASET.weather_raw already exists" >&2
fi

echo "Initialization complete."

