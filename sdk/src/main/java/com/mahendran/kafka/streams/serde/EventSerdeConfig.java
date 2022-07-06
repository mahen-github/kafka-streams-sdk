package com.mahendran.kafka.streams.serde;

import java.util.HashMap;
import java.util.Map;

public final class EventSerdeConfig {

  static final Map<String, Object> producerConfiguration = new HashMap<>();

  static final Map<String, Object> consumerConfiguration = new HashMap<>();

  private EventSerdeConfig() {
    throw new AssertionError("you must not instantiate this class");
  }

  /**
   * Enables the use of Specific Avro.
   *
   * @param config the serializer/deserializer/serde configuration
   * @return a copy of the configuration with the user specified configuration is enabled
   */
  public static Map<String, Object> withProducerConfig(final Map<String, ?> config) {
    if (config == null || config.isEmpty()) {
      return producerConfiguration;
    } else {
      producerConfiguration.putAll(config);
    }
    return producerConfiguration;
  }

  /**
   * Enables the use of Specific Avro.
   *
   * @param config the serializer/deserializer/serde configuration
   * @return a copy of the configuration with the user specified configuration is enabled
   */
  public static Map<String, Object> withConsumerConfig(final Map<String, ?> config) {
    if (config == null || config.isEmpty()) {
      return consumerConfiguration;
    } else {
      consumerConfiguration.putAll(config);
    }
    return consumerConfiguration;
  }
}
