package com.mahendran.kafka.consumer;

import com.mahendran.kafka.streams.KafkaConfig;
import com.mahendran.kafka.streams.KafkaConfig.ClientType;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.basicauth.UserInfoCredentialProvider;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public class Consumer {

  private final KafkaConsumer<String, KafkaAvroDeserializer> consumer;
  private final Properties props;

  public Consumer(Properties props) {
    this.props = props;
    consumer = new KafkaConsumer<>(props);
  }

  public KafkaConsumer<String, KafkaAvroDeserializer> getConsumer() {
    return consumer;
  }

  public static final class Config {

    private static final int IDENTITY_MAP_CAPACITY = 5;
    private final Properties properties;
    private RestService restService;

    /**
     * Constructor that takes user defined properties.
     *
     * @param cluster map of user defined properties
     */
    public Config(@NotNull KafkaConfig.Cluster cluster) {
      properties = new Properties();
      properties.putAll(consumerConfigDefault());
      properties.putAll(KafkaConfig.brokerAndSchemaRegistryConfig(cluster, ClientType.CONSUMER));
      init(properties);
    }

    /**
     * Constructor that takes user defined properties.
     *
     * @param protonClusterProps map of user defined properties
     */
    public Config(@NotNull Map<String, String> protonClusterProps) {
      properties = new Properties();
      properties.putAll(consumerConfigDefault());
      properties.putAll(protonClusterProps);
      init(properties);
    }

    private static Properties consumerConfigDefault() {
      Properties props = new Properties();
      props.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      props.put(
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
      props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
      props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

      return props;
    }

    private void init(Properties props) {
      String schemaRegistryUrl =
          properties.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
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
     * @return this class
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

    public Config withTopicNameStrategy(String strategyName) {
      properties.put(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, strategyName);
      return this;
    }

    /**
     * // More control over batching - defaults replicate Kafka documentation defaults. // See
     * https://kafka.apache.org/documentation/
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
     */
    public Consumer build() {
      return new Consumer(properties);
    }
  }
}
