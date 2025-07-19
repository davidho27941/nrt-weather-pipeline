package com.example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.stream.Collectors;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A simple streaming pipeline that reads station IDs from Pub/Sub, fetches
 * weather data from the CWA API, and writes the results to Cloud Storage and
 * BigQuery.
 */
public class WeatherPipeline {

  /** Pipeline options for the Weather pipeline. */
  public interface Options extends DataflowPipelineOptions {
    @Description("Input Pub/Sub topic")
    String getInputTopic();
    void setInputTopic(String value);

    @Description("GCS output prefix (e.g. gs://bucket/path/prefix)")
    String getOutputPath();
    void setOutputPath(String value);

    @Description("BigQuery table (<project>:<dataset>.<table>)")
    String getBigQueryTable();
    void setBigQueryTable(String value);

    @Description("API token for CWA")
    String getApiToken();
    void setApiToken(String value);
  }

  /** DoFn that calls the CWA API for a given station ID. */
  static class FetchWeatherDoFn extends DoFn<String, String> {
    private final String apiToken;
    private final Gson gson = new Gson();

    FetchWeatherDoFn(String apiToken) {
      this.apiToken = apiToken;
    }

    @ProcessElement
    public void processElement(@Element String stationId, OutputReceiver<String> out) {
      try {
        String url = String.format(
            "https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0001-001" +
            "?Authorization=%s&limit=1&StationId=%s",
            URLEncoder.encode(apiToken, StandardCharsets.UTF_8.name()),
            URLEncoder.encode(stationId, StandardCharsets.UTF_8.name()));
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("GET");
        try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
          String response = reader.lines().collect(Collectors.joining("\n"));
          // Output the raw JSON string.
          out.output(response);
        }
      } catch (IOException e) {
        System.err.println("Failed to fetch data for station " + stationId + ": " + e.getMessage());
      }
    }
  }

  /** Main entry point for the pipeline. */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setStreaming(true);

    Pipeline pipeline = Pipeline.create(options);

    // Read station IDs from Pub/Sub.
    pipeline
        .apply("ReadStationId", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
        // Fetch weather data from the API.
        .apply("FetchWeather", ParDo.of(new FetchWeatherDoFn(options.getApiToken())))
        // Write raw JSON to Cloud Storage.
        .apply("WriteToGCS", TextIO.write().to(options.getOutputPath()).withSuffix(".json"))
        // Convert to TableRow for BigQuery.
        .apply("ToTableRow",
            MapElements.into(TypeDescriptor.of(TableRow.class))
                .via(json -> new TableRow().set("raw_json", json)))
        // Write to BigQuery.
        .apply(
            "WriteToBQ",
            BigQueryIO.writeTableRows()
                .to(options.getBigQueryTable())
                .withSchema(
                    new TableSchema().setFields(
                        Collections.singletonList(
                            new TableFieldSchema().setName("raw_json").setType("STRING"))))
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

    pipeline.run();
  }
}
