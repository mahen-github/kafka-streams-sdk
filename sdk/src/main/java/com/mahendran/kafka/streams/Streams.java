package com.mahendran.kafka.streams;

import com.mahendran.kafka.streams.KafkaConfig.ClientType;
import com.mahendran.kafka.streams.KafkaConfig.Cluster;
import com.mahendran.kafka.streams.serde.EventSerdeConfig;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.basicauth.UserInfoCredentialProvider;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public class Streams {

  private final Config config;
  private Topology topology;

  public Streams(@NotNull Config config) {
    this.config = config;
  }

  public Config getConfig() {
    return config;
  }

  /**
   * Set the topology.
   *
   * @param topology topology for streams
   * @return this class
   */
  public Streams withTopology(@NotNull Topology topology) {
    this.topology = topology;
    return this;
  }

  /**
   * Creates  {@link KafkaStreams} instance with the given {@link Topology topology}.
   *
   * @return {@link KafkaStreams}
   */
  public KafkaStreams createStreams() {
    Objects.requireNonNull(topology,
        "internal error: cannot build empty topology. "
            + "Use \"withTopology\" method to set the topology.");
    KafkaStreams streams = new KafkaStreams(topology, config.properties);
    return streams;
  }

  public static final class Config {

    private static final int IDENTITY_MAP_CAPACITY = 5;
    private final Properties properties;
    private SchemaRegistryClient schemaRegistryClient;
    private String schemaRegistryUrl;
    private RestService restService;

    /**
     * Constructor that takes user defined properties.
     *
     * @param cluster map of user defined properties
     */
    public Config(@NotNull Cluster cluster) {
      properties = new Properties();
      properties.putAll(streamsConfigDefault());
      properties.putAll(consumerConfigFor(cluster).getProperties());
      init(properties);
    }

    /**
     * Constructor that takes user defined properties.
     *
     * @param protonClusterProps map of user defined properties
     */
    public Config(@NotNull Map<String, String> protonClusterProps) {
      properties = new Properties();
      properties.putAll(streamsConfigDefault());
      properties.putAll(protonClusterProps);
      init(properties);
    }

    private static Config consumerConfigFor(Cluster cluster) {
      return new Config(KafkaConfig.brokerAndSchemaRegistryConfig(cluster, ClientType.STREAMS));
    }

    private static Properties streamsConfigDefault() {
      Properties props = new Properties();
      props.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
      props.put(StreamsConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
      props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
      //In order to use the SpecificAvroSerde, use withSpecificAvroSerde()
      props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName());
      return props;
    }

    private void init(Properties props) {
      schemaRegistryUrl = props
          .getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
      restService = new RestService(schemaRegistryUrl);
    }

    public Properties getProperties() {
      return properties;
    }

    /**
     * Adds the specified API keys to this {@code Config}. This is necessary in order to use a
     * Proton cluster.
     *
     * @param accessKey the API access key
     * @param secretKey the API secret key
     * @return the updated {@code Config} object
     */
    @Contract("_, _ -> this")
    public Config withApiKeys(String accessKey, String secretKey) {
      Objects.requireNonNull(accessKey, "accessKey must not be null");
      Objects.requireNonNull(secretKey, "secretKey must not be null");

      BasicAuthCredentialProvider provider = new UserInfoCredentialProvider();
      provider.configure(
          Map.of(SchemaRegistryClientConfig.USER_INFO_CONFIG, accessKey + ":" + secretKey));
      restService.setBasicAuthCredentialProvider(provider);

      properties.putAll(KafkaConfig.secureConfig(accessKey, secretKey));
      return this;
    }

    @Contract(" _ -> this")
    public Config withCustomSerde(String clazzName) {
      properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, clazzName);
      return this;
    }

    /**
     * Enable the SpecificAvroSerde.
     */
    @Contract(" -> this")
    public Config withSpecificAvroSerde() {
      properties
          .put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
      properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
      return this;
    }

    /**
     * Setup multi schema consumer.
     *
     * @return this
     */
    @Contract(" -> this")
    public Config withMultiSchemaConsumerConfig() {
      properties.putAll(EventSerdeConfig
          .withConsumerConfig(Map.of(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY,
              TopicRecordNameStrategy.class.getName())));
      return this;
    }

    /**
     * Setup multi schema producer.
     *
     * @return this
     */
    @Contract(" -> this")
    public Config withMultiSchemaProducerConfig() {
      properties.putAll(EventSerdeConfig
          .withProducerConfig(Map.of(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY,
              TopicRecordNameStrategy.class.getName())));
      return this;
    }

    /**
     * Override application id.
     *
     * @return this
     */
    @Contract("_ -> this")
    public Config withApplicationId(String applicationId) {
      Objects.requireNonNull(applicationId, "applicationId must not be null.");
      properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
      return this;
    }

    /**
     * Override client id.
     *
     * @return this
     */
    @Contract("_ -> this")
    public Config withClientId(String clientId) {
      Objects.requireNonNull(clientId, "clientId must not be null.");
      properties.put(StreamsConfig.CLIENT_ID_CONFIG, clientId);
      return this;
    }

    /**
     * Create and set instance of {@link SchemaRegistryClient}.
     *
     * @return this
     */
    @Contract(" -> this")
    public Config withSchemaRegistry() {
      Objects.requireNonNull(schemaRegistryUrl, "must provide the schema registry url");
      if (schemaRegistryClient == null) {
        schemaRegistryClient = new CachedSchemaRegistryClient(restService, IDENTITY_MAP_CAPACITY);
      }
      return this;
    }

    /**
     * Create and set instance of {@link SchemaRegistryClient}.
     *
     * @return Instance of schema registry client
     */
    public SchemaRegistryClient getSchemaRegistryClient() {
      if (schemaRegistryClient == null) {
        schemaRegistryClient = new CachedSchemaRegistryClient(restService, IDENTITY_MAP_CAPACITY);
      }
      return schemaRegistryClient;
    }

    /**
     * Set and set instance of {@link SchemaRegistryClient}.
     *
     * @param client User provided schema registry client
     */
    public void setSchemaRegistryClient(SchemaRegistryClient client) {
      schemaRegistryClient = client;
    }

    public Streams build() {
      return new Streams(this);
    }
  }
}
