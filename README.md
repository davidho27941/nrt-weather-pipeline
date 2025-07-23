# nrt-weather-pipeline

A simple streaming pipeline prototype for ingesting weather data. The pipeline
reads station IDs from a Pub/Sub topic, queries the Central Weather Administration
(CWA) API, and writes the responses to both Cloud Storage and BigQuery.
API requests are retried a configurable number of times and the station IDs are
emitted as a `PCollectionTuple` separating successful and failed calls.

## Building

This project uses Maven. To build a shaded JAR:

```bash
mvn package
```

## Environment Setup

Create the required Pub/Sub topic, Cloud Storage bucket and BigQuery table before launching the pipeline. The commands below assume you have the `gcloud`, `gsutil` and `bq` CLIs installed and authenticated.

```bash
# configure your project
gcloud config set project <gcp-project>

# create the Pub/Sub topic that will contain station IDs
gcloud pubsub topics create weather_stn_id

# create the Pub/Sub topic that will receive failed IDs
gcloud pubsub topics create weather_retry

# create a bucket for Dataflow output
gsutil mb -l <region> gs://<bucket>

# create a BigQuery dataset and table for raw JSON
bq --location=<region> mk <dataset>
bq mk --table <dataset>.weather_raw raw_json:STRING
```

All of the above steps can be executed automatically using the `init.sh` script provided in this repository:

```bash
./init.sh <gcp-project> <region> <bucket> <dataset>
```

## Running on Dataflow

Use Maven's `exec:java` goal to launch the pipeline:

```bash
mvn compile exec:java \
  -Dexec.mainClass=com.example.WeatherPipeline \
  -Dexec.args="--runner=DataflowRunner \
    --project=<gcp-project> \
    --region=<region> \
    --inputTopic=projects/<project>/topics/weather_stn_id \
    --outputPath=gs://<bucket>/weather/output \
    --bigQueryTable=<project>:<dataset>.weather_raw \
    --apiToken=<api-token> \
    --retryAttempts=<retries>"
```

Alternatively, you can use the included helper script:

```bash
./run.sh <gcp-project> <region> <bucket> <dataset> <api-token> <retries> [trust-store-secret-id] [trust-store-secret-version]
```

If the optional trust store arguments are omitted, the script defaults to `cwa-trust-pem` for the secret ID
and `latest` for the secret version.
The `<retries>` parameter controls how many times a failed API request will be retried.

The pipeline expects the Pub/Sub topic `weather_stn_id` to contain
station IDs as plain strings. Failed IDs are published to the
`weather_retry` topic for further processing.

## Cleanup

When you are finished you can remove the created resources with the
`tear_down.sh` script. It deletes the Pub/Sub topic, Cloud Storage bucket
and BigQuery dataset by default. Individual resources can be skipped
using the flags shown below. The topic name can also be overridden.

```bash
./tear_down.sh <gcp-project> <bucket> <dataset> [--topic weather_stn_id] \
  [--retry-topic weather_retry] [--no-topic] [--no-retry-topic] \
  [--no-bucket] [--no-dataset]
```
