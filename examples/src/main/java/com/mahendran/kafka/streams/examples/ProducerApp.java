package com.mahendran.kafka.streams.examples;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import com.mahendran.kafka.producer.GenericProducer;
import java.util.Properties;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerConfig;

public class ProducerApp {

  public static void main(String[] args) {
    ProducerApp producerApp = new ProducerApp();
    producerApp.produce();
  }

  private <T extends SpecificRecordBase & SpecificRecord> void produce() {

    var props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
    props.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
    try (GenericProducer<String, T> genericProducer = new GenericProducer<>(props)) {
//      genericProducer.produce();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
