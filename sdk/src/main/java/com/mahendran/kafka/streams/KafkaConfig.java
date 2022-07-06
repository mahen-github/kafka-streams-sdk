package com.mahendran.kafka.streams;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.jetbrains.annotations.NotNull;

public final class KafkaConfig {

  private static final String LOCAL_BOOTSTRAP = "localhost:9092";
  private static final String LOCAL_SCHEMA_REGISTRY = "http://localhost:8081";

  private static final String SCRAM_SHA_512 = "SCRAM-SHA-512";
  private static final String SASL_SSL = "SASL_SSL";
  private static final String SASL_INHERIT = "SASL_INHERIT";
  private static final String SASL_JAAS_CONFIG_TEMPLATE =
      "org.apache.kafka.common.security.scram.ScramLoginModule"
          + " required username=\"%s\" password=\"%s\";";

  /**
   * Provides defaults for a connection to kafka cluster.
   *
   * @param cluster the named cluster
   * @param configType the type of client
   * @return properties for Proton cluster connection
   */
  @NotNull
  public static Map<String, String> brokerAndSchemaRegistryConfig(
      Cluster cluster, ClientType configType) {
    return Map.of(
        configType.bootstrapConfig,
        cluster.getBootstrap(),
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        cluster.getSchemaRegistry());
  }

  /**
   * Provides defaults for a secure connection to kafka cluster.
   *
   * @return properties for secure connection
   */
  @NotNull
  public static Map<String, String> secureConfig(String accessKey, String secretKey) {
    Map<String, String> defaults = new HashMap<>();

    defaults.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, SASL_SSL);
    defaults.put(SaslConfigs.SASL_MECHANISM, SCRAM_SHA_512);
    defaults.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, SASL_INHERIT);
    defaults.put(
        SaslConfigs.SASL_JAAS_CONFIG,
        String.format(SASL_JAAS_CONFIG_TEMPLATE, accessKey, secretKey));

    return defaults;
  }

  public enum ClientType {
    STREAMS(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG),
    PRODUCER(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
    CONSUMER(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);

    private final String bootstrapConfig;

    ClientType(String bootstrapConfig) {
      this.bootstrapConfig = bootstrapConfig;
    }
  }

  public enum Cluster {
    LOCAL(KafkaConfig.LOCAL_BOOTSTRAP, KafkaConfig.LOCAL_SCHEMA_REGISTRY);

    private final String bootstrap;
    private final String schemaRegistry;

    Cluster(String bootstrap, String schemaRegistry) {
      this.bootstrap = bootstrap;
      this.schemaRegistry = schemaRegistry;
    }

    public String getBootstrap() {
      return bootstrap;
    }

    public String getSchemaRegistry() {
      return schemaRegistry;
    }
  }
}
