package com.mahendran.kafka;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.basicauth.SaslBasicAuthCredentialProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.jetbrains.annotations.NotNull;

public class SchemaRegistryClient {

  public SchemaRegistryClient(String schemaRegistryUrl) {
    this.properties = new Properties();
    this.restService = new RestService(schemaRegistryUrl);
      this.schemaRegistryClient = new CachedSchemaRegistryClient(this.restService, 5);
  }

  private final Properties properties;
  private io.confluent.kafka.schemaregistry.client.SchemaRegistryClient schemaRegistryClient;
  private RestService restService;

  public Properties getProperties() {
    return this.properties;
  }

  public io.confluent.kafka.schemaregistry.client.SchemaRegistryClient getSchemaRegistryClient() {
    Objects.requireNonNull(schemaRegistryClient, "schemaRegistryClient is not initialized");
    return schemaRegistryClient;
  }

  public RestService getRestService() {
    return restService;
  }


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
}
