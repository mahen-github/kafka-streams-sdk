package com.mahendran.kafka.streams.serde;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class EventSerde<T extends SpecificRecordBase & SpecificRecord> implements Serde<T> {

  final Serde<T> inner;

  /**
   * Default constructor required by KafkaAvroDeserializer.
   */
  public EventSerde() {
    inner = Serdes.serdeFrom(
        new EventSerializer<>(),
        new EventDeserializer<>());
  }

  /**
   * For testing purposes only.
   */
  public EventSerde(final SchemaRegistryClient client) {
    if (client == null) {
      throw new IllegalArgumentException("schema registry client must not be null");
    }
    inner = Serdes.serdeFrom(
        new EventSerializer<>(client),
        new EventDeserializer<>(client));
  }

  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
    inner.serializer().configure(props, isKey);
    inner.deserializer().configure(props, isKey);
  }

  @Override
  public Serializer<T> serializer() {
    return inner.serializer();
  }

  @Override
  public Deserializer<T> deserializer() {
    return inner.deserializer();
  }

  @Override
  public void close() {
    inner.serializer().close();
    inner.deserializer().close();
  }
}

