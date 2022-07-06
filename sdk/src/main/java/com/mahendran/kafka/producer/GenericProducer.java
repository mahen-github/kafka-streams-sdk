package com.mahendran.kafka.producer;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_REQUEST_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.basicauth.SaslBasicAuthCredentialProvider;
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
  private  Properties properties;
  private SchemaRegistryClient schemaRegistryClient;
  private RestService restService;

  public Properties getProperties() {
    return properties;
  }

  public SchemaRegistryClient getSchemaRegistryClient() {
    Objects.requireNonNull(schemaRegistryClient, "schemaRegistryClient is not initialized");
    return schemaRegistryClient;
  }

  public RestService getRestService() {
    return restService;
  }

  public KafkaProducer<K, V> getProducer() {
    return producer;
  }

  private KafkaProducer<K, V> producer;

  public void withApiKeys(String accessKey, String secretKey) {
    Objects.requireNonNull(accessKey, "accessKey must not be null");
    Objects.requireNonNull(secretKey, "secretKey must not be null");
    BasicAuthCredentialProvider provider = new SaslBasicAuthCredentialProvider();
    provider.configure(Map.of("sasl.jaas.config", String.format("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";", accessKey, secretKey)));
    this.restService.setBasicAuthCredentialProvider(provider);
    this.properties.putAll(secureConfig(accessKey, secretKey));
  }

  @NotNull
  static Map<String, String> secureConfig(String accessKey, String secretKey) {
    Map<String, String> defaults = new HashMap();
    defaults.put("security.protocol", "SASL_SSL");
    defaults.put("sasl.mechanism", "SCRAM-SHA-512");
    defaults.put("basic.auth.credentials.source", "SASL_INHERIT");
    defaults.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";", accessKey, secretKey));
    return defaults;
  }

  /**
   * A {@link GenericRecord} kafka producer.
   *
   */
  public void producer() {
    var props = new Properties();
    props.put("auto.register.schemas", false);
    props.put("application.id", UUID.randomUUID().toString());
    props.put("client.id", UUID.randomUUID().toString());
    props.put("specific.avro.reader", true);
    props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put(MAX_REQUEST_SIZE_CONFIG, 1024 * 1024 * 10);
    props.put(AUTO_REGISTER_SCHEMAS, true);
    this.properties = new Properties(props);
    producer = new KafkaProducer<>(props);
  }


  public void withSchemaRegistry(String schemaRegistryUrl ) {
    Objects.requireNonNull(schemaRegistryUrl, "must provide the schema registry url");
    this.restService = new RestService(schemaRegistryUrl);
    if (this.schemaRegistryClient == null) {
      this.schemaRegistryClient = new CachedSchemaRegistryClient(this.restService, 5);
    }
  }

  /** Publishes a local event with KafkaProducer. */
  void produce(V event, K key, String eventType, String topic, int noOfEvents) {
    try {
      // TODO: Async threads
      for (int i = 0; i < noOfEvents; i++) {
        ProducerRecord<K, V> recordTo = new ProducerRecord<>(topic, key, event);
        addMessageHeader(recordTo, eventType);
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
      }
      producer.flush();
    } catch (Exception e) {
      throw new RuntimeException("Failed to produce event: " + ExceptionUtils.getStackTrace(e), e);
    }
  }

  /** Creates a header for the event and sets sample values. */
  private void addMessageHeader(ProducerRecord<K, V> recordTo, String eventType) {
    long timestamp = Instant.now().toEpochMilli();
    var headers = recordTo.headers();

    BiConsumer<String, String> addToHeader =
        (key, value) -> headers.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
    addToHeader.accept("ID", eventType + "-" + Instant.now());
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
