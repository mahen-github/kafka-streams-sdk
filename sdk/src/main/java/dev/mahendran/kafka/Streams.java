package dev.mahendran.kafka;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static org.apache.kafka.clients.admin.AdminClientConfig.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/** Creates a {@link org.apache.kafka.streams.kstream.KStream} instance. */
public class Streams {

  private static final String SASL_JAAS_OAUTH_CONFIG_STRING =
      "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
          + "clientId=\"%s\" clientSecret=\"%s\" scope=\"user\";";
  private final KafkaStreams streams;

  public Streams(@NotNull Topology topology, @NotNull Properties properties) {
    this.streams = new KafkaStreams(topology, properties);
  }

  /**
   * Get the instance of {@link KafkaStreams}.
   *
   * @return KafkaStreams
   */
  public KafkaStreams getStreams() {
    return streams;
  }

  /** A config class to provide default properties. */
  public static final class Config {

    private final Properties properties;

    /** Constructor that takes user defined properties. */
    public Config(@NotNull Map<String, String> config) {
      properties = new Properties();
      properties.putAll(config);
    }

    private static Properties streamsConfigDefault() {
      Properties props = new Properties();
      props.put(AUTO_REGISTER_SCHEMAS, false);
      props.put(APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
      props.put(CLIENT_ID_CONFIG, UUID.randomUUID().toString());
      props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
      props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
      return props;
    }

    /**
     * Adds the specified API keys to this {@code Config}. This is necessary in order to use a
     * Proton cluster.
     *
     * @param accessKey the API access key
     * @param secretKey the API secret key
     * @return this class
     */
    @Contract("_, _ -> this")
    public static Map<String, String> getApiKeyProps(String accessKey, String secretKey) {
      Objects.requireNonNull(accessKey, "accessKey must not be null");
      Objects.requireNonNull(secretKey, "secretKey must not be null");

      Map<String, String> defaults = new HashMap<>();
      defaults.put("security.protocol", "SASL_SSL");
      defaults.put("sasl.mechanism", "SCRAM-SHA-512");
      defaults.put("basic.auth.credentials.source", "SASL_INHERIT");
      defaults.put(
          "sasl.jaas.config",
          String.format(
              "org.apache.kafka.common.security.scram.ScramLoginModule"
                  + " required username=\"%s\" password=\"%s\";",
              accessKey, secretKey));

      return defaults;
    }

    /**
     * Get OAuth Properties to add to the defualt props.
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

    public Properties getProperties() {
      return properties;
    }
  }
}
