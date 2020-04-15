package com.mahendran.kafka.streams.serde;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

/**
 * EventSerde class is an implementation of the Serde for specificRecord. This class enables reading
 * the avro record with schema. Avro generated java class must present for the schema in the
 * application.
 *
 * <p>This class enable reading from a multi schema topic and produce to default topic strategy.
 */
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
  public EventSerde(SchemaRegistryClient client) {
    if (client == null) {
      throw new IllegalArgumentException("schema registry client must not be null");
    }
    inner = Serdes.serdeFrom(
        new EventSerializer<>(client),
        new EventDeserializer<>(client));
  }

  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
    Map<String, Object> properties = new HashMap<>(props);
    properties.putAll(props);
    //Enable the specific avro serde
    properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    inner.serializer().configure(properties, isKey);
    inner.deserializer().configure(properties, isKey);
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

