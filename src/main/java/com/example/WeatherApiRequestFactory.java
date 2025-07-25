package com.example;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import okhttp3.HttpUrl;
import okhttp3.Request;

/** Utility for building requests to the CWA API based on pipeline options. */
public class WeatherApiRequestFactory {
  private final HttpUrl baseUrl;
  private final String apiToken;
  private final List<String> defaultParamGroups;
  private final Map<String, String> headers;
  private final boolean tokenInHeader;
  private final String tokenHeaderName;

  public WeatherApiRequestFactory(
      String baseUrl,
      String apiToken,
      List<String> defaultParamGroups,
      String headerString,
      boolean tokenInHeader,
      String tokenHeaderName) {
    this.baseUrl = HttpUrl.parse(baseUrl);
    this.apiToken = apiToken;
    this.defaultParamGroups = defaultParamGroups == null ? new ArrayList<>() : defaultParamGroups;
    this.headers = parseHeaders(headerString);
    this.tokenInHeader = tokenInHeader;
    this.tokenHeaderName = tokenHeaderName == null ? "Authorization" : tokenHeaderName;
  }

  /** Build an OkHttp {@link Request} for the given station ID. */
  public Request buildRequest(String stationId) {
    HttpUrl.Builder builder = baseUrl.newBuilder();

    // Apply default query parameters from all groups
    for (String group : defaultParamGroups) {
      if (group == null || group.isEmpty()) {
        continue;
      }
      String[] pairs = group.split("&");
      for (String pair : pairs) {
        if (pair.isEmpty()) {
          continue;
        }
        String[] kv = pair.split("=", 2);
        String key = decode(kv[0]);
        String value = kv.length > 1 ? decode(kv[1]) : "";
        builder.addQueryParameter(key, value);
      }
    }

    builder.addQueryParameter("StationId", stationId);

    if (!tokenInHeader) {
      builder.addQueryParameter("Authorization", apiToken);
    }

    Request.Builder reqBuilder = new Request.Builder().url(builder.build()).get();

    if (tokenInHeader) {
      reqBuilder.addHeader(tokenHeaderName, apiToken);
    }
    for (Map.Entry<String, String> e : headers.entrySet()) {
      reqBuilder.addHeader(e.getKey(), e.getValue());
    }

    return reqBuilder.build();
  }

  private static Map<String, String> parseHeaders(String headerString) {
    Map<String, String> map = new HashMap<>();
    if (headerString == null || headerString.isEmpty()) {
      return map;
    }
    String[] pairs = headerString.split(",");
    for (String pair : pairs) {
      String[] kv = pair.trim().split(":", 2);
      if (kv.length == 2) {
        map.put(kv[0].trim(), kv[1].trim());
      }
    }
    return map;
  }

  private static String decode(String s) {
    return URLDecoder.decode(s, StandardCharsets.UTF_8);
  }
}
