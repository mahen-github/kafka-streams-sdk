package com.mahendran.kafka.producer;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_REQUEST_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BiConsumer;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;

/**
 * A Generic record producer rto produce records of types including {@link
 * org.apache.avro.specific.SpecificRecordBase}, {@link GenericRecord}.
 */
public class GenericProducer<K, V> implements AutoCloseable {

  private final KafkaProducer<K, V> producer;
  private final Properties properties;
  private Properties defaultProperties;

  /**
   * Instantiate producer with the producer properties.
   *
   * <p> User are expected to provide properties with BootStrap Server ad Schema Registry URL.
   *
   * @param properties Kafka Producer properties
   */
  public GenericProducer(Properties properties) {
    //get the default props
    this.properties = new Properties(defaultProperties());
    //add user provided props. User provided config will override the default props
    this.properties.putAll(properties);
    producer = new KafkaProducer<>(properties);
  }

  /**
   * Instantiate producer with the user provided properties, default properties and SASL key and
   * secret.
   *
   * <p> User are expected to provide properties with BootStrap Server ad Schema Registry URL.
   *
   * @param properties Kafka Producer properties
   */
  public GenericProducer(Properties properties, String accessKey, String secretKey) {
    //get the default props
    this.properties = new Properties(defaultProperties());
    // add SASL
    this.properties.putAll(withApiKeys(accessKey, secretKey));
    //add user provided props. User provided config will override the default props
    this.properties.putAll(properties);
    producer = new KafkaProducer<>(properties);
  }

  @NotNull
  static Map<String, String> secureConfig(String accessKey, String secretKey) {
    Map<String, String> defaults = new HashMap();
    defaults.put("security.protocol", "SASL_SSL");
    defaults.put("sasl.mechanism", "SCRAM-SHA-512");
    defaults.put("basic.auth.credentials.source", "SASL_INHERIT");
    defaults.put("sasl.jaas.config", String.format(
        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
        accessKey, secretKey));
    return defaults;
  }

  public Properties getProperties() {
    return properties;
  }

  public Properties getDefaultProperties() {
    return defaultProperties;
  }

  public KafkaProducer<K, V> getProducer() {
    return producer;
  }

  public Properties withApiKeys(String accessKey, String secretKey) {
    Objects.requireNonNull(accessKey, "accessKey must not be null");
    Objects.requireNonNull(secretKey, "secretKey must not be null");
    this.properties.putAll(secureConfig(accessKey, secretKey));
    this.defaultProperties.putAll(secureConfig(accessKey, secretKey));
    return defaultProperties;
  }

  private Properties defaultProperties() {
    this.defaultProperties = new Properties();
    defaultProperties.put(AUTO_REGISTER_SCHEMAS, false);
    defaultProperties.put(SPECIFIC_AVRO_READER_CONFIG, true);
    defaultProperties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    defaultProperties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG,
        KafkaAvroSerializer.class.getName());
    defaultProperties.put(MAX_REQUEST_SIZE_CONFIG, 1024 * 1024 * 10); // Messages up to 10 MB
    return defaultProperties;
  }

  /**
   * Publishes a local event with KafkaProducer.
   */
  public void produce(V event, K key, String topic) {
    try {
      // TODO: Async threads
      ProducerRecord<K, V> recordTo = new ProducerRecord<>(topic, key, event);
      addMessageHeader(recordTo);
      producer.send(
          recordTo,
          (recordMetadata, exception) -> {
            if (exception != null) {
              throw new RuntimeException(
                  "Failed to produce messages " + ExceptionUtils.getStackTrace(exception),
                  exception);
            } else {
              System.out.println(recordMetadata.offset());
            }
          });
    } catch (Exception e) {
      throw new RuntimeException("Failed to produce event: " + ExceptionUtils.getStackTrace(e), e);
    }
  }

  /**
   * Creates a header for the event and sets sample values.
   */
  private void addMessageHeader(ProducerRecord<K, V> recordTo) {
    long timestamp = Instant.now().toEpochMilli();
    var headers = recordTo.headers();

    BiConsumer<String, String> addToHeader =
        (key, value) -> headers.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
    addToHeader.accept("ID", UUID.randomUUID().toString());
    addToHeader.accept("APP_ID", "APP_ID");
    addToHeader.accept("EVENT_TIME", Long.toString(timestamp));
    addToHeader.accept("SYSTEM_TIME", Long.toString(timestamp));
  }

  @Override
  public void close() throws Exception {
    if (producer != null) {
      producer.close();
    }
  }

}
