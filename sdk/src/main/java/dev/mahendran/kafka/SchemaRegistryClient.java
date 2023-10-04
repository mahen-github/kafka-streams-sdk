package dev.mahendran.kafka;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.basicauth.SaslBasicAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.basicauth.UserInfoCredentialProvider;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/** A {@link CachedSchemaRegistryClient} instance. */
public class SchemaRegistryClient {

  private Properties properties;
  private io.confluent.kafka.schemaregistry.client.SchemaRegistryClient schemaRegistryClient;
  private RestService restService;

  /**
   * Default Constructor.
   *
   * @param schemaRegistryUrl The schema registry url
   */
  public SchemaRegistryClient(String schemaRegistryUrl) {
    this.restService = new RestService(schemaRegistryUrl);
    this.schemaRegistryClient = new CachedSchemaRegistryClient(this.restService, 5);
  }

  /**
   * Constructor with schemaRegistryUrl and user provided properties.
   *
   * @param schemaRegistryUrl schemaRegistryUrl
   * @param properties user defined properties
   */
  public SchemaRegistryClient(String schemaRegistryUrl, Map<String, String> properties) {
    this.restService = new RestService(schemaRegistryUrl);
    this.schemaRegistryClient = new CachedSchemaRegistryClient(this.restService, 5);
    BasicAuthCredentialProvider provider = new UserInfoCredentialProvider();
    provider.configure(properties);
    restService.setBasicAuthCredentialProvider(provider);
  }

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

  /**
   * {@code SaslBasicAuthCredentialProvider} takes API key and secret.
   *
   * @param accessKey API access key
   * @param secretKey API secret
   */
  public void withSaslBasicAuthCredentialProvider(String accessKey, String secretKey) {
    BasicAuthCredentialProvider provider = new SaslBasicAuthCredentialProvider();
    provider.configure(
        Map.of(
            "sasl.jaas.config",
            String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule"
                    + " required username=\"%s\" password=\"%s\";",
                accessKey, secretKey)));
    this.restService.setBasicAuthCredentialProvider(provider);
  }

  /**
   * {@code UserInfoCredentialProvider} takes API key and secret.
   *
   * @param accessKey API access key
   * @param secretKey API secret
   */
  public void withUserBasicAuthCredentialProvider(String accessKey, String secretKey) {
    BasicAuthCredentialProvider provider = new UserInfoCredentialProvider();
    provider.configure(
        Map.of(SchemaRegistryClientConfig.USER_INFO_CONFIG, accessKey + ":" + secretKey));
    restService.setBasicAuthCredentialProvider(provider);
  }
}
