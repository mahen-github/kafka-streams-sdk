package com.mahendran.kafka.streams.serde;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;

public class EventDeserializer<T extends SpecificRecordBase & SpecificRecord>
    implements Deserializer<T> {

  private final KafkaAvroDeserializer inner;

  public EventDeserializer() {
    inner = new KafkaAvroDeserializer();
  }

  /** For testing purposes only. */
  EventDeserializer(final SchemaRegistryClient client) {
    EventSerdeConfig.withConsumerConfig(
        Map.of(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, client));
    inner = new KafkaAvroDeserializer(client);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    inner.configure(EventSerdeConfig.withConsumerConfig(configs), isKey);
  }

  @Override
  @SuppressWarnings(value = "unchecked")
  public T deserialize(String topic, byte[] bytes) {
    return (T)
        inner.deserialize(topic, bytes, (Schema) EventSerdeConfig.producerConfiguration.get(topic));
  }
}
