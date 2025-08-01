# nrt-weather-pipeline

A simple streaming pipeline prototype for ingesting weather data. The pipeline
reads station IDs from a Pub/Sub topic, queries the Central Weather Administration
(CWA) API, and writes the responses to both Cloud Storage and BigQuery.
API requests are retried a configurable number of times and the station IDs are
emitted as a `PCollectionTuple` separating successful and failed calls.

## Project Overview

This repository contains a small Apache Beam application that runs on Google
Cloud Dataflow. It demonstrates how to fetch real-time weather data from the
CWA open data API and persist the results for further processing. The pipeline
makes use of the following Google Cloud resources:

- **Pub/Sub topics** – `weather_stn_id` provides station IDs to process and
  `weather_retry` receives IDs that failed after all retry attempts.
- **Cloud Storage bucket** – used to store raw JSON output files.
- **BigQuery dataset and table** – each successful API response is stored in a
  table called `weather_raw` for querying.
- **Secret Manager** – stores the PEM trust store required for calling the API
  over HTTPS.
- **Dataflow** – runs the streaming Beam job that orchestrates the workflow.

The following diagram outlines the main flow of data through the system.

```mermaid
flowchart LR
    subgraph "Dataflow job"
        A["Pub/Sub\nweather_stn_id"] --> B["FetchWeatherDoFn\nquery CWA API"]
        B -->|"success JSON"| C["Cloud Storage"]
        B -->|"success JSON"| D["BigQuery"]
        B -->|"failed ID"| E["Pub/Sub\nweather_retry"]
    end
    E -.-> F["Retry Dataflow job"]
```

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

To reuse the same JAR for a retry workflow, simply provide a different
`--inputTopic` and set a unique `--jobName` when launching the pipeline. For
example:

```bash
mvn compile exec:java \
  -Dexec.mainClass=com.example.WeatherPipeline \
  -Dexec.args="--runner=DataflowRunner \
    --project=<gcp-project> \
    --region=<region> \
    --inputTopic=projects/<project>/topics/weather_retry \
    --jobName=weather-pipeline-retry \
    --outputPath=gs://<bucket>/weather/output \
    --bigQueryTable=<project>:<dataset>.weather_raw \
    --apiToken=<api-token> \
    --retryAttempts=<retries>"
```

Alternatively, you can use the included helper script:

```bash
./run.sh <gcp-project> <region> <bucket> <dataset> <api-token> <retries> [trust-store-secret-id] [trust-store-secret-version]
```

Additional settings can be provided via environment variables before
running the script:

- `API_BASE_URL` - override the CWA API endpoint
- `API_DEFAULT_PARAM` - default query parameters, e.g. `limit=1&elementName=TIME`
- `API_HEADERS` - extra HTTP headers as `Header:Value` pairs separated by commas
- `TOKEN_IN_HEADER` - set to `true` to send the API token in a header
- `TOKEN_HEADER_NAME` - header name used when `TOKEN_IN_HEADER` is `true`
- `INPUT_TOPIC` - Pub/Sub topic to read station IDs from
- `JOB_NAME` - name for the Dataflow job
- `NUM_WORKERS` - fixed number of Dataflow workers
- `MAX_WORKERS` - maximum number of Dataflow workers

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
