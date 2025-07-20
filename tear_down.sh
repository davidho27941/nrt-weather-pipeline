#!/bin/bash
set -euo pipefail

usage() {
  echo "Usage: $0 <project-id> <bucket> <dataset> [--topic name] [--no-topic] [--no-bucket] [--no-dataset]" >&2
  exit 1
}

if [ $# -lt 3 ]; then
  usage
fi

PROJECT_ID=$1
BUCKET=$2
DATASET=$3
shift 3

TOPIC="weather_stn_id"
DELETE_TOPIC=true
DELETE_BUCKET=true
DELETE_DATASET=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --topic)
      TOPIC="$2"
      shift 2
      ;;
    --no-topic)
      DELETE_TOPIC=false
      shift
      ;;
    --no-bucket)
      DELETE_BUCKET=false
      shift
      ;;
    --no-dataset)
      DELETE_DATASET=false
      shift
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      ;;
  esac
done

echo "Configuring project $PROJECT_ID"
gcloud config set project "$PROJECT_ID" >&2

if $DELETE_TOPIC; then
  if gcloud pubsub topics describe "$TOPIC" >/dev/null 2>&1; then
    gcloud pubsub topics delete "$TOPIC"
  else
    echo "Pub/Sub topic $TOPIC does not exist" >&2
  fi
fi

if $DELETE_BUCKET; then
  if gsutil ls -b "gs://$BUCKET" >/dev/null 2>&1; then
    gsutil -m rm -r "gs://$BUCKET"
  else
    echo "Bucket gs://$BUCKET does not exist" >&2
  fi
fi

if $DELETE_DATASET; then
  if bq show "$DATASET.weather_raw" >/dev/null 2>&1; then
    bq rm -f -t "$DATASET.weather_raw"
  fi
  if bq show "$DATASET" >/dev/null 2>&1; then
    bq rm -f -d "$DATASET"
  else
    echo "Dataset $DATASET does not exist" >&2
  fi
fi

echo "Teardown complete."
