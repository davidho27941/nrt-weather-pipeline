package com.example;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;

import com.example.FetchWeatherDoFn;
import org.joda.time.Duration;

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

    @Description("Secret ID containing CWA trust store PEM")
    String getTrustStoreSecretId();
    void setTrustStoreSecretId(String value);

    @Description("Secret version for the trust store")
    String getTrustStoreSecretVersion();
    void setTrustStoreSecretVersion(String value);

    @Description("Number of retry attempts for failed API requests")
    @Default.Integer(3)
    Integer getRetryAttempts();
    void setRetryAttempts(Integer value);
  }

  /** Main entry point for the pipeline. */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setStreaming(true);

    Pipeline pipeline = Pipeline.create(options);

    PCollectionTuple result =
        pipeline
            .apply("ReadStationId", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
            .apply(
                "FetchWeather",
                ParDo.of(
                        new FetchWeatherDoFn(
                            options.getApiToken(),
                            options.getProject(),
                            options.getTrustStoreSecretId(),
                            options.getTrustStoreSecretVersion(),
                            options.getRetryAttempts()))
                    .withOutputTags(
                        FetchWeatherDoFn.JSON_TAG,
                        TupleTagList.of(FetchWeatherDoFn.SUCCESS_TAG)
                            .and(FetchWeatherDoFn.FAILED_TAG)));

    result
        .get(FetchWeatherDoFn.JSON_TAG)
        .apply("FixedWindow", Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))))
        .apply(
            "WriteToGCS",
            TextIO.write()
                .withWindowedWrites()
                .withNumShards(1)
                .to(options.getOutputPath())
                .withSuffix(".json"));

    // Publish failed IDs to a retry Pub/Sub topic
    result
        .get(FetchWeatherDoFn.FAILED_TAG)
        .apply(
            "PublishFailedIds",
            PubsubIO.writeStrings()
                .to(String.format("projects/%s/topics/weather_retry", options.getProject())));

    // The success and failed ID collections are available as
    // result.get(FetchWeatherDoFn.SUCCESS_TAG) and result.get(FetchWeatherDoFn.FAILED_TAG)
    // for further processing.

    pipeline.run();
  }
}
