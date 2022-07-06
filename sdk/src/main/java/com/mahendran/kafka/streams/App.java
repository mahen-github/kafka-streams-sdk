package com.mahendran.kafka.streams;

import com.mahendran.kafka.streams.KafkaConfig.Cluster;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

public class App {

  private static Streams streams;

  /** main method to run the streams. */
  public static void main(String[] args) {

    streams = new Streams.Config(Cluster.LOCAL).build();
    KafkaStreams streams = App.streams.setTopology(getTopology()).createStreams();
    streams.start();
    streams.cleanUp();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  private static Topology getTopology() {
    StreamsBuilder builder = new StreamsBuilder();
    builder.stream("topic").to("");
    return builder.build(streams.getConfig().getProperties());
  }
}
