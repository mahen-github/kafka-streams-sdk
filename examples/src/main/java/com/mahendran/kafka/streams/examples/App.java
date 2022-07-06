package com.mahendran.kafka.streams.examples;

import com.mahendran.kafka.streams.KafkaConfig.Cluster;
import com.mahendran.kafka.streams.Streams;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

/**
 * Demonstrates, using the {@link Streams} API. This example uses lambda expressions and thus works
 * with Java 8+ only.
 *
 * <p>In this example, the input stream reads from a topic named "customer_input_avro" and produces
 * to "customer_output_avro".
 *
 * <p><br>
 * HOW TO RUN THIS EXAMPLE
 *
 * <p>
 *
 * <ol>
 *   <li>Create the source topic.
 *       <pre>(e.g. via {@code kafka-topics --create ...})</pre>
 *   <li>Start this example.
 *       <pre>
 * {@code java -jar examples/build/libs/examples-1.0-SNAPSHOT.jar com.mahendran.kafka.streams
 * .examples.App}
 * </pre>
 *   <li>Write some data to the source topic.
 *       <pre>
 *   (e.g. via {@code kafka-console-producer}).
 * </pre>
 *   <li>Inspect the resulting data in the output topic.
 *       <pre>
 * {@code confluent-5.4.1/bin/kafka-avro-console-consumer --bootstrap-server localhost:9092 \
 *                                    --property schema.registry.url=http://localhost:8081 \
 *                                    --topic customer_output_avro \
 *                                    --from-beginning
 * }
 * </pre>
 * </ol>
 */
public class App {

  private static Streams streams;

  /** main method to run the streams. */
  public static void main(String[] args) {

    streams = new Streams.Config(Cluster.LOCAL).build();

    KafkaStreams streams = App.streams.setTopology(getTopology()).createStreams();
    streams.cleanUp();
    streams.start();
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  private static Topology getTopology() {
    StreamsBuilder builder = new StreamsBuilder();
    builder.stream("customer_input_avro").to("customer_output_avro");
    return builder.build(streams.getConfig().getProperties());
  }
}
