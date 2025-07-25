package com.example;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

/** Utility class for JSON serialization and deserialization using Gson. */
public final class JsonUtil {
  private static final Gson GSON = new Gson();

  private JsonUtil() {}

  /** Serialize the given object into its JSON representation. */
  public static String toJson(Object src) {
    return GSON.toJson(src);
  }

  /** Deserialize the JSON string into an instance of the specified class. */
  public static <T> T fromJson(String json, Class<T> clazz) {
    return GSON.fromJson(json, clazz);
  }

  /** Deserialize the JSON using a {@link TypeToken}. */
  public static <T> T fromJson(String json, TypeToken<T> token) {
    return GSON.fromJson(json, token.getType());
  }

  /** Parse a JSON string into a {@link JsonElement}. */
  public static JsonElement parse(String json) {
    return GSON.fromJson(json, JsonElement.class);
  }
}
