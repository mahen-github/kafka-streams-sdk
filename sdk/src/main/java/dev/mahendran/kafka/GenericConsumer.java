package dev.mahendran.kafka;

import static io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS;
import static io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jetbrains.annotations.NotNull;

/**
 * A Genreic kafka consumer with K and V type.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class GenericConsumer<K, V> {

  private static final String SASL_JAAS_CONFIG_TEMPLATE =
      "org.apache.kafka.common.security.scram.ScramLoginModule"
          + " required username=\"%s\" password=\"%s\";";

  private final KafkaConsumer<K, V> kafkaConsumer;
  private final Properties props;

  public GenericConsumer(Properties props) {
    this.props = props;
    kafkaConsumer = new KafkaConsumer<>(props);
  }

  public KafkaConsumer<K, V> getKafkaConsumer() {
    return kafkaConsumer;
  }

  /** A static config class to provide default props. */
  public static final class Config {

    private final Properties properties;

    /** Constructor that takes user defined properties. */
    public Config(@NotNull Map<String, String> config) {
      properties = new Properties();
      properties.putAll(config);
    }

    /**
     * Return default Kafka Consumer Properties.
     *
     * <p>More control over batching - defaults replicate Kafka documentation defaults.
     *
     * <p>See https://kafka.apache.org/documentation/
     * props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,args.enableAutoCommit);
     * props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, args.minFetchBytes);
     * props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, args.maxFetchBytes);
     * props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, args.maxPartitionFetchBytes);
     * props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, args.maxPollRecords);
     * props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, args.maxFetchWait);
     * props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, args.maxPollInterval);
     * props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, args.sessionTimeoutMs);
     * props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, args.heartbeatIntervalMs);
     * props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, args.connectionsMaxIdleMs);
     * props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, args.defaultApiTimeoutMs);
     *
     * @return Consumer Properties
     */
    public static Properties defaultProperties() {
      Properties props = new Properties();
      props.put(AUTO_REGISTER_SCHEMAS, false);
      // Default serializers for key and value. Override when its different
      props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      props.put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
      // Should be replaced with a meaning ful consumer group id
      props.put(GROUP_ID_CONFIG, UUID.randomUUID().toString());
      props.put(AUTO_OFFSET_RESET_CONFIG, EARLIEST);
      props.put(SPECIFIC_AVRO_READER_CONFIG, true);

      return props;
    }

    /**
     * Provides defaults for a secure connection to kafka cluster.
     *
     * @return properties for secure connection
     */
    @NotNull
    public static Properties getScramProperties(String accessKey, String secretKey) {
      Properties scarmProperties = new Properties();
      scarmProperties.put(SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
      scarmProperties.put(SASL_MECHANISM, "SCRAM-SHA-512");
      scarmProperties.put(BASIC_AUTH_CREDENTIALS_SOURCE, "SASL_INHERIT");
      scarmProperties.put(
          SASL_JAAS_CONFIG, String.format(SASL_JAAS_CONFIG_TEMPLATE, accessKey, secretKey));

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
    public static Properties getOuthProperties(
        String clientId, String secret, String oauthEndpoint) {
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
     * Properties.
     *
     * @return Properties
     */
    public Properties getProperties() {
      return properties;
    }
  }
}
