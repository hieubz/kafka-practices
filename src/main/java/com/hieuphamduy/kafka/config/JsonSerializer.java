package com.hieuphamduy.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerializer<T> implements Serializer<T> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public byte[] serialize(final String topic, T data) {
    try {
      return OBJECT_MAPPER.writeValueAsBytes(data);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot serialize object to json");
    }
  }
}
