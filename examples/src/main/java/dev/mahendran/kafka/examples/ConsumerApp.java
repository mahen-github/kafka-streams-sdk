package dev.mahendran.kafka.examples;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

import dev.mahendran.kafka.GenericConsumer;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * A customer Consumer app consumes the {@link dev.mahendran.event.Customer} events from a kafka
 * topic.
 */
public class ConsumerApp {

  /** main method to run the streams. */
  public static void main(String[] args) {

    var boostStrapServer = "";
    var schemaRegistryUrl = "";
    var clientId = "";
    var secret = "";
    var oauthEndPoint = "";

    var props = GenericConsumer.Config.defaultProperties();

    props.put(BOOTSTRAP_SERVERS_CONFIG, boostStrapServer);
    props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

    // Skip this for the local kafka cluster
    props.putAll(GenericConsumer.Config.getOuthProperties(clientId, secret, oauthEndPoint));

    var consumer = new GenericConsumer<String, dev.mahendran.event.Customer>(props);
    var kafkaConsumer = consumer.getKafkaConsumer();
    var topicName = "";
    kafkaConsumer.subscribe(List.of(topicName));
    while (true) {
      var records = kafkaConsumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, dev.mahendran.event.Customer> record : records) {
        System.out.println(record.value());
      }
    }
  }
}
