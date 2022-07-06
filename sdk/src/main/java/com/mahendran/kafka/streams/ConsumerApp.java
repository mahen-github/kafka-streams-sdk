// package com.mahendran.kafka.streams;
//
// import com.mahendran.kafka.streams.KafkaConfig.Cluster;
// import java.time.Duration;
// import org.apache.avro.generic.GenericRecord;
// import org.apache.kafka.common.serialization.Serdes;
// import org.apache.kafka.streams.KafkaStreams;
// import org.apache.kafka.streams.StreamsBuilder;
// import org.apache.kafka.streams.Topology;
// import org.apache.kafka.streams.kstream.KStream;
// import org.apache.kafka.streams.kstream.TimeWindowedKStream;
// import org.apache.kafka.streams.kstream.TimeWindows;
// import org.apache.kafka.streams.processor.Processor;
// import org.apache.kafka.streams.processor.ProcessorContext;
// import org.apache.kafka.streams.processor.To;
// import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
// import org.apache.kafka.streams.state.KeyValueStore;
// import org.apache.kafka.streams.state.StoreBuilder;
// import org.apache.kafka.streams.state.Stores;
//
// public class ConsumerApp {
//
//  private static Streams streams;
//
//  /** main method to run the streams. */
//  public static void main(String[] args) {
//    //
//    //    KafkaConsumer<String, KafkaAvroDeserializer> con = new Consumer
//    //        .Config(Cluster.LOCAL)
//    //        .build()
//    //        .getConsumer();
//
//    streams = new Streams.Config(Cluster.LOCAL).build();
//    KafkaStreams streams = ConsumerApp.streams.setTopology(getTopology()).createStreams();
//    streams.cleanUp();
//    streams.start();
//
//    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
//  }
//
//  private static Topology getTopology() {
//    StreamsBuilder builder = new StreamsBuilder();
//    KStream<String, GenericRecord> inputStream = builder.stream("customer");
//    inputStream.process(ConsumerApp::get);
//    Duration windowSizeMs = Duration.ofMinutes(2);
//    Duration advanceMs = Duration.ofSeconds(5);
//    KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore("store");
//
//    Topology topology = new Topology();
//    topology.addStateStore(storeSupplier, "");
//    TimeWindows timeWindows = TimeWindows.of(windowSizeMs).advanceBy(advanceMs);
//
//    TimeWindowedKStream<String, GenericRecord> timedWindowStream =
//        inputStream.groupByKey().windowedBy(timeWindows);
//
//    timedWindowStream
//        .reduce(
//            (value1, value2) -> {
//              System.out.println(value1 + "=== > " + value2);
//              return null;
//            })
//        .toStream();
//
//    StoreBuilder<KeyValueStore<String, Long>> wordCountsStore =
//        Stores.keyValueStoreBuilder(
//                Stores.keyValueStoreBuilder( new KeyValueBytesStoreSupplier(), Serdes.String(),
// Serdes.Long())
//            .withCachingEnabled();
//
//    var storeBuilder =
//        Stores.keyValueStoreBuilder(
//            Stores.inMemoryKeyValueStore(""), Serdes.String(), Serdes.ByteArray());
//
//    return builder.build(streams.getConfig().getProperties());
//  }
//
//  private static Processor<String, GenericRecord> get() {
//    return new Processor<>() {
//      private ProcessorContext context;
//
//      @Override
//      public void init(ProcessorContext context) {
//        context = context;
//      }
//
//      @Override
//      public void process(String key, GenericRecord value) {
//        System.out.println(key);
//        System.out.println(value);
//        context.forward(key, value, To.child(ConsumerApp.get1()));
//      }
//
//      @Override
//      public void close() {}
//    };
//  }
//
//  private static Processor<String, GenericRecord> get1() {
//    return new Processor<>() {
//      @Override
//      public void init(ProcessorContext context) {}
//
//      @Override
//      public void process(String key, GenericRecord value) {
//        System.out.println(key);
//        System.out.println(value);
//      }
//
//      @Override
//      public void close() {}
//    };
//  }
// }
