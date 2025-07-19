# nrt-weather-pipeline

A simple streaming pipeline prototype for ingesting weather data. The pipeline
reads station IDs from a Pub/Sub topic, queries the Central Weather Administration
(CWA) API, and writes the responses to both Cloud Storage and BigQuery.

## Building

This project uses Maven. To build a shaded JAR:

```bash
mvn package
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

The pipeline expects the Pub/Sub topic `weather_stn_id` to contain
station IDs as plain strings.
