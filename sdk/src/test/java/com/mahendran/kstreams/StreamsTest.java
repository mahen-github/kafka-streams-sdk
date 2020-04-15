package com.mahendran.kstreams;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.mahendran.kafka.streams.KafkaConfig.Cluster;
import com.mahendran.kafka.streams.Streams;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

class StreamsTest {

  private static Schema getSchema() {
    return new Schema.Parser().parse(
        "{\"namespace\":\"com.mahendran.poc.kafka\",\"name\":\"Customer\",\"type\":\"record\","
            + "\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}");
  }

  @Test
  void properties_withDefault() {
    var streams = new Streams.Config(Cluster.LOCAL).build();
    Properties props = streams.getConfig().getProperties();
    assertAll(
        () -> assertEquals("http://localhost:8081", props.get("schema.registry.url")),
        () -> assertEquals("false", props.get("auto.register.schemas").toString()),
        () -> assertEquals("io.confluent.kafka.streams.serdes.avro.GenericAvroSerde",
            props.get("default.value.serde")),
        () -> assertEquals("localhost:9092", props.get("bootstrap.servers")),
        () -> assertEquals("org.apache.kafka.common.serialization.Serdes$StringSerde",
            props.get("default.key.serde")),
        () -> assertNotNull(props.get("application.id")),
        () -> assertNotNull(props.get("client.id")),
        () -> assertNull(props.get("basic.auth.credentials.source")),
        () -> assertNull(props.get("security.protocol")),
        () -> assertNull(props.get("sasl.mechanism")),
        () -> assertNull(props.get("sasl.jaas.config"))
    );
  }

  @Test
  void properties_WithSasl() {

    var config = new Streams.Config(Cluster.LOCAL)
        .withApiKeys("SASL_ACCESS_KEY", "SASL_SECRET");
    Properties props = config.getProperties();
    assertAll(
        () -> assertEquals("SASL_INHERIT", props.get("basic.auth.credentials.source")),
        () -> assertEquals("SASL_SSL", props.get("security.protocol")),
        () -> assertEquals("SCRAM-SHA-512", props.get("sasl.mechanism")),
        () -> assertEquals("http://localhost:8081", props.get("schema.registry.url")),
        () -> assertEquals("false", props.get("auto.register.schemas").toString()),
        () -> assertEquals("io.confluent.kafka.streams.serdes.avro.GenericAvroSerde",
            props.get("default.value.serde")),
        () -> assertEquals("localhost:9092", props.get("bootstrap.servers")),
        () -> assertEquals("org.apache.kafka.common.serialization.Serdes$StringSerde",
            props.get("default.key.serde")),
        () -> assertNotNull(props.get("application.id")),
        () -> assertNotNull(props.get("client.id")),
        () -> assertEquals(
            "org.apache.kafka.common.security.scram.ScramLoginModule required"
                + " username=\"SASL_ACCESS_KEY\" password=\"SASL_SECRET\";",
            props.get("sasl.jaas.config"))
    );
  }

  @Test
  void properties_WithUserDefinedProps() {
    Map<String, String> inputProps = new HashMap<>();
    inputProps.put("bootstrap.servers", "localhost:9092");
    inputProps.put("schema.registry.url", "http://localhost:8081");

    var config = new Streams.Config(inputProps)
        .withApiKeys("SASL_ACCESS_KEY", "SASL_SECRET");
    Properties props = config.getProperties();
    assertAll(
        () -> assertEquals("SASL_INHERIT", props.get("basic.auth.credentials.source")),
        () -> assertEquals("SASL_SSL", props.get("security.protocol")),
        () -> assertEquals("SCRAM-SHA-512", props.get("sasl.mechanism")),
        () -> assertEquals("http://localhost:8081", props.get("schema.registry.url")),
        () -> assertEquals("false", props.get("auto.register.schemas").toString()),
        () -> assertEquals("io.confluent.kafka.streams.serdes.avro.GenericAvroSerde",
            props.get("default.value.serde")),
        () -> assertEquals("localhost:9092", props.get("bootstrap.servers")),
        () -> assertEquals("org.apache.kafka.common.serialization.Serdes$StringSerde",
            props.get("default.key.serde")),
        () -> assertNotNull(props.get("application.id")),
        () -> assertNotNull(props.get("client.id")),
        () -> assertEquals(
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
                + "username=\"SASL_ACCESS_KEY\" password=\"SASL_SECRET\";",
            props.get("sasl.jaas.config"))
    );
  }

  @Test
  void properties_WithConsumerConfig() {
    var streams = new Streams.Config(Cluster.LOCAL).withMultiSchemaConsumerConfig();
    Properties props = streams.getProperties();
    assertEquals(TopicRecordNameStrategy.class.getName(),
        props.get(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY));
  }

  @Test
  void properties_WithDefaultConfig() {
    var streams = new Streams.Config(Cluster.LOCAL);
    Properties props = streams.getProperties();
    assertNull(props.get(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY));
  }

  @Test
  void properties_WithProducerConfig() {
    var streams = new Streams.Config(Cluster.LOCAL).withMultiSchemaProducerConfig();
    Properties props = streams.getProperties();
    assertEquals(TopicRecordNameStrategy.class.getName(),
        props.get(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY));
  }

  @Test
  void properties_WithSchemaRegistryClient() throws IOException, RestClientException {
    var streams = new Streams.Config(Cluster.LOCAL).withSchemaRegistry();
    MockSchemaRegistryClient client = new MockSchemaRegistryClient();
    client.register("subject1", getSchema());
    client.register("subject2", getSchema());
    streams.setSchemaRegistryClient(client);
    Properties props = streams.getProperties();
    assertEquals("http://localhost:8081", props.get("schema.registry.url"));
    var expectedClient = streams.getSchemaRegistryClient();
    assertEquals(List.of("subject1", "subject2"), expectedClient.getAllSubjects());
  }

  @Test
  void properties_setSchemaRegistryClient() throws IOException, RestClientException {

    var streams = new Streams.Config(Cluster.LOCAL);
    MockSchemaRegistryClient client = new MockSchemaRegistryClient();
    client.register("subject1", getSchema());
    client.register("subject2", getSchema());
    streams.setSchemaRegistryClient(client);
    Properties props = streams.getProperties();
    assertEquals("http://localhost:8081", props.get("schema.registry.url"));
    var expectedClient = streams.getSchemaRegistryClient();
    assertEquals(List.of("subject1", "subject2"), expectedClient.getAllSubjects());
  }

  @Test
  void properties_withCustomSerde() {
    var streams = new Streams.Config(Cluster.LOCAL)
        .withCustomSerde(GenericAvroSerde.class.getName());
    Properties props = streams.getProperties();
    assertEquals("io.confluent.kafka.streams.serdes.avro.GenericAvroSerde",
        props.get("default.value.serde"));
  }

  @Test
  void properties_withApiKeys() {
    var streams = new Streams.Config(Cluster.LOCAL)
        .withApiKeys("access", "secret");
    Properties props = streams.getProperties();
    assertAll(
        () -> assertEquals("SASL_INHERIT", props.get("basic.auth.credentials.source")),
        () -> assertEquals("SASL_SSL", props.get("security.protocol")),
        () -> assertEquals("SCRAM-SHA-512", props.get("sasl.mechanism")),
        () -> assertEquals(
            "org.apache.kafka.common.security.scram.ScramLoginModule "
                + "required username=\"access\" password=\"secret\";",
            props.get("sasl.jaas.config")),
        () -> assertThrows(NullPointerException.class,
            () -> streams.withApiKeys(null, "secret")),
        () -> assertThrows(NullPointerException.class,
            () -> streams.withApiKeys("access", null))
    );
  }

  @Test
  void streams_whenMandatoryFieldsAreNull() {
    assertAll(
        () -> assertThrows(NullPointerException.class,
            () -> new Streams.Config(Cluster.LOCAL).build().createStreams())
    );
  }

  @Test
  void config_whenClientIdIsNull() {
    assertAll(
        () -> assertThrows(NullPointerException.class,
            () -> new Streams.Config(Cluster.LOCAL).withClientId(null)),
        () -> assertThrows(NullPointerException.class,
            () -> new Streams.Config(Cluster.LOCAL).withApplicationId(null))
    );
  }
}