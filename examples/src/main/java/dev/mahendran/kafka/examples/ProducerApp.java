package dev.mahendran.kafka.examples;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import dev.mahendran.event.Customer;
import dev.mahendran.kafka.GenericProducer;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerConfig;

/** A customer event producer app. */
public class ProducerApp {

  public static void main(String[] args) {
    ProducerApp producerApp = new ProducerApp();
    producerApp.produce();
  }

  private void produce() {

    var topicName = "";
    var props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
    props.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

    Customer customer = getCustomer();

    // Skip this line for local cluster
    props.putAll(GenericProducer.getOuthProperties("", "", ""));

    try (GenericProducer<String, Customer> genericProducer = new GenericProducer<>(props)) {
      genericProducer.produce(customer, customer.getCustomerId().toString(), topicName);
    } catch (Exception e) {
      throw new RuntimeException("Failed to produce", e);
    }
  }

  private Customer getCustomer() {

    return Customer.newBuilder()
        .setCustomerId(UUID.randomUUID().toString())
        .setAge(67)
        .setEventTime(Instant.EPOCH)
        .setFirstName("Pablo")
        .setLastName("Escobar")
        .build();
  }
}
