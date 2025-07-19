package com.example;

import com.google.gson.Gson;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.logging.Logger;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okhttp3.logging.HttpLoggingInterceptor;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

/**
 * A simple streaming pipeline that reads station IDs from Pub/Sub, fetches
 * weather data from the CWA API, and writes the results to Cloud Storage and
 * BigQuery.
 */
public class WeatherPipeline {
  private final Gson gson = new Gson();

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
    private static final Logger logger = Logger.getLogger(FetchWeatherDoFn.class.getName());
    private final String apiToken;
    private transient OkHttpClient httpClient;

    FetchWeatherDoFn(String apiToken) {
      this.apiToken = apiToken;
    }

    @Setup
    public void setup() {
      // Optional: add an HTTP logging interceptor for debugging
      HttpLoggingInterceptor logging = new HttpLoggingInterceptor();
      logging.setLevel(HttpLoggingInterceptor.Level.BASIC);

      this.httpClient = new OkHttpClient.Builder()
          .connectTimeout(java.time.Duration.ofSeconds(10))
          .readTimeout(java.time.Duration.ofSeconds(20))
          .addInterceptor(logging)
          .build();
    }

    @ProcessElement
    public void processElement(@Element String stationId, OutputReceiver<String> out) {
      logger.info("Received station ID: " + stationId);
      try {
        String url = String.format(
            "https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0001-001" +
            "?Authorization=%s&limit=1&StationId=%s",
            URLEncoder.encode(apiToken, StandardCharsets.UTF_8.name()),
            URLEncoder.encode(stationId, StandardCharsets.UTF_8.name()));
        Request request = new Request.Builder()
            .url(url)
            .get()
            .build();

        try (Response response = httpClient.newCall(request).execute()) {
          if (!response.isSuccessful()) {
            logger.warning("Fetch failed for station " + stationId +
                           " with HTTP " + response.code());
            return;
          }
          ResponseBody body = response.body();
          if (body != null) {
            String json = body.string();
            out.output(json);
          } else {
            logger.warning("Empty response body for station " + stationId);
          }
        }
      } catch (IOException e) {
        logger.severe("Failed to fetch data for station " + stationId + ": " + e.getMessage());
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
	.apply("FixedWindow", Window.<String>into(
          FixedWindows.of(Duration.standardMinutes(1))))
        // 4. **开启 windowed writes** 并指定分片数
        .apply("WriteToGCS", TextIO.write()
          .withWindowedWrites()           // 必须！
          .withNumShards(1)               // 可按需调整
          .to(options.getOutputPath())    
          .withSuffix(".json"));

    pipeline.run();
  }
}
