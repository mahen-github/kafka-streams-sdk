package dev.mahendran.kafka;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_REQUEST_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.nio.charset.StandardCharsets;
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
  private Properties defaultProperties;

  /**
   * Instantiate producer with the producer properties.
   *
   * <p>User are expected to provide properties with BootStrap Server ad Schema Registry URL.
   *
   * @param properties Kafka Producer properties
   */
  public GenericProducer(Properties properties) {
    producer = new KafkaProducer<>(properties);
  }

  /**
   * Provides defaults for a secure connection to kafka cluster.
   *
   * @return properties for secure connection
   */
  @NotNull
  public static Properties getScramProperties(String accessKey, String secretKey) {
    Objects.requireNonNull(accessKey, "accessKey must not be null");
    Objects.requireNonNull(secretKey, "secretKey must not be null");
    Properties scarmProperties = new Properties();
    scarmProperties.put(SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    scarmProperties.put(SASL_MECHANISM, "SCRAM-SHA-512");
    scarmProperties.put(BASIC_AUTH_CREDENTIALS_SOURCE, "SASL_INHERIT");
    scarmProperties.put(
        SASL_JAAS_CONFIG,
        String.format(
            "org.apache.kafka.common.security.scram.ScramLoginModule required"
                + " username=\"%s\" password=\"%s\";",
            accessKey, secretKey));

    return scarmProperties;
  }

  /**
   * Get OAuth Properties to add to the default props.
   *
   * @param clientId client Id
   * @param secret Secret
   * @param oauthEndpoint OAuth end point
   * @return OAuth Properties
   */
  public static Properties getOuthProperties(String clientId, String secret, String oauthEndpoint) {
    Properties oauthProperties = new Properties();
    oauthProperties.put("kafka.request.timeout.ms", "20000");
    oauthProperties.put(SASL_LOGIN_RETRY_BACKOFF_MS, "500");
    oauthProperties.put(SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    oauthProperties.put(
        SASL_JAAS_CONFIG,
        String.format(
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required"
                + " clientId=\"%s\""
                + " clientSecret=\"%s\""
                + " scope=\"user\";",
            clientId, secret));
    oauthProperties.put(SASL_MECHANISM, "OAUTHBEARER");
    oauthProperties.put(
        "sasl.login.callback.handler.class",
        "org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler");
    oauthProperties.put("sasl.oauthbearer.token.endpoint.url", oauthEndpoint);
    oauthProperties.put("bearer.auth.logical.cluster", "not-used");
    oauthProperties.put("bearer.auth.credentials.source", "SASL_OAUTHBEARER_INHERIT");

    return oauthProperties;
  }

  /**
   * Return the default producer properties.
   *
   * @return properties
   */
  public Properties getDefaultProperties() {
    if (defaultProperties == null) {
      defaultProperties = getDefaultProperties();
    }
    return defaultProperties;
  }

  public KafkaProducer<K, V> getProducer() {
    return producer;
  }

  private Properties defaultProperties() {
    this.defaultProperties = new Properties();
    defaultProperties.put(AUTO_REGISTER_SCHEMAS, false);
    defaultProperties.put(SPECIFIC_AVRO_READER_CONFIG, true);
    defaultProperties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    defaultProperties.setProperty(
        VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    defaultProperties.put(MAX_REQUEST_SIZE_CONFIG, 1024 * 1024 * 10); // Messages up to 10 MB
    return defaultProperties;
  }

  /** Publishes a local event with KafkaProducer. */
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

  /** Creates a header for the event and sets sample values. */
  private void addMessageHeader(ProducerRecord<K, V> recordTo) {
    var headers = recordTo.headers();

    BiConsumer<String, String> addToHeader =
        (key, value) -> headers.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
    addToHeader.accept("ID", UUID.randomUUID().toString());
  }

  @Override
  public void close() {
    if (producer != null) {
      producer.close();
    }
  }
}
