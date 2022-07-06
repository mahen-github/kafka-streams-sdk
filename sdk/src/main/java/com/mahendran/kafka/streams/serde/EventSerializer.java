package com.mahendran.kafka.streams.serde;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

public class EventSerializer<T extends SpecificRecordBase & SpecificRecord>
    implements Serializer<T> {

  private final KafkaAvroSerializer inner;

  public EventSerializer() {
    this.inner = new KafkaAvroSerializer();
  }

  /** For testing purposes only. */
  EventSerializer(final SchemaRegistryClient client) {
    EventSerdeConfig.withProducerConfig(
        Map.of(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "test:8081"));
    inner = new KafkaAvroSerializer(client);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    inner.configure(EventSerdeConfig.withProducerConfig(configs), isKey);
  }

  @Override
  public byte[] serialize(final String topic, final T record) {
    return inner.serialize(topic, record);
  }
}
