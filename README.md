# nrt-weather-pipeline

A simple streaming pipeline prototype for ingesting weather data. The pipeline
reads station IDs from a Pub/Sub topic, queries the Central Weather Administration
(CWA) API, and writes the responses to both Cloud Storage and BigQuery.

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
    --apiToken=<api-token>"
```

Alternatively, you can use the included helper script:

```bash
./run.sh <gcp-project> <region> <bucket> <dataset> <api-token> [trust-store-secret-id] [trust-store-secret-version]
```

If the optional trust store arguments are omitted, the script defaults to `cwa-trust-pem` for the secret ID
and `latest` for the secret version.

The pipeline expects the Pub/Sub topic `weather_stn_id` to contain
station IDs as plain strings.
