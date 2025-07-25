package com.example;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okhttp3.logging.HttpLoggingInterceptor;
import com.example.WeatherApiRequestFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.values.TupleTag;

/** DoFn that calls the CWA API for a given station ID. */
public class FetchWeatherDoFn extends DoFn<String, String> {
  public static final TupleTag<String> JSON_TAG = new TupleTag<String>() {};
  public static final TupleTag<String> SUCCESS_TAG = new TupleTag<String>() {};
  public static final TupleTag<String> FAILED_TAG = new TupleTag<String>() {};

  private static final Logger logger = Logger.getLogger(FetchWeatherDoFn.class.getName());
  private final WeatherApiRequestFactory requestFactory;
  private final String projectId;
  private final String secretId;
  private final String secretVersion;
  private final int retryAttempts;
  private transient OkHttpClient httpClient;

  public FetchWeatherDoFn(
      WeatherApiRequestFactory requestFactory,
      String projectId,
      String secretId,
      String secretVersion,
      int retryAttempts) {
    this.requestFactory = requestFactory;
    this.projectId = projectId;
    this.secretId = (secretId == null || secretId.isEmpty()) ? "cwa-trust-pem" : secretId;
    this.secretVersion = (secretVersion == null || secretVersion.isEmpty()) ? "latest" : secretVersion;
    this.retryAttempts = retryAttempts;
  }

  @Setup
  public void setup() throws Exception {
    // 1. Fetch PEM bytes from Secret Manager
    SecretVersionName name = SecretVersionName.of(projectId, secretId, secretVersion);
    byte[] pemBytes;
    try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
      AccessSecretVersionResponse resp = client.accessSecretVersion(name);
      pemBytes = resp.getPayload().getData().toByteArray();
    }

    // 2. Parse PEM and build KeyStore
    CertificateFactory cf = CertificateFactory.getInstance("X.509");
    Certificate cert;
    try (ByteArrayInputStream bis = new ByteArrayInputStream(pemBytes)) {
      cert = cf.generateCertificate(bis);
    }

    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    ks.load(null, null);
    ks.setCertificateEntry("cwa-opendata", cert);
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ks);

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, tmf.getTrustManagers(), null);

    HttpLoggingInterceptor logging = new HttpLoggingInterceptor();
    logging.setLevel(HttpLoggingInterceptor.Level.BASIC);

    this.httpClient =
        new OkHttpClient.Builder()
            .sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) tmf.getTrustManagers()[0])
            .hostnameVerifier((h, s) -> true)
            .connectTimeout(java.time.Duration.ofSeconds(10))
            .readTimeout(java.time.Duration.ofSeconds(20))
            .addInterceptor(logging)
            .build();
  }

  @ProcessElement
  public void processElement(@Element String stationId, MultiOutputReceiver out) {
    logger.info("Received station ID: " + stationId);
    int attempt = 0;
    while (true) {
      try {
        Request request = requestFactory.buildRequest(stationId);

        try (Response response = httpClient.newCall(request).execute()) {
          if (response.isSuccessful()) {
            ResponseBody body = response.body();
            if (body != null) {
              String json = body.string();
              boolean hasRecords = false;
              try {
                JsonElement root = JsonUtil.parse(json);
                if (root.isJsonObject()) {
                  JsonObject obj = root.getAsJsonObject();
                  JsonObject records = obj.getAsJsonObject("records");
                  if (records != null) {
                    JsonArray stationArr = records.getAsJsonArray("Station");
                    hasRecords = stationArr != null && stationArr.size() > 0;
                  }
                }
              } catch (Exception e) {
                logger.warning(
                    "Failed to parse JSON for station " + stationId + ": " + e.getMessage());
              }

              if (hasRecords) {
                out.get(JSON_TAG).output(json);
                out.get(SUCCESS_TAG).output(stationId);
                return;
              } else {
                logger.warning("Empty records.Station for station " + stationId);
              }
            } else {
              logger.warning("Empty response body for station " + stationId);
            }
          } else {
            logger.warning(
                "Fetch failed for station " + stationId + " with HTTP " + response.code());
          }
        }
      } catch (IOException e) {
        logger.severe(
            "Failed to fetch data for station " + stationId + ": " + e.getMessage());
      }

      if (attempt >= retryAttempts) {
        out.get(FAILED_TAG).output(stationId);
        return;
      }
      attempt++;
    }
  }
}
