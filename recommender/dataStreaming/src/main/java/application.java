import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class application {

    public static void main(String[] args) {
        String brokers = "localhost:9092";
        String zookeepers = "localhost:2181";

        // 输入和输出的topic
        String from = "spotify_log";
        String to = "spotify_recommender";

        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        StreamsConfig config = new StreamsConfig(settings);

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("SOURCE", from)
                .addProcessor("PROCESSOR", new LogProcessorSupplier(), "SOURCE")
                .addSink("SINK", to, "PROCESSOR");

        KafkaStreams streams = new KafkaStreams( builder, config );

        streams.start();

        System.out.println("Kafka stream started!>>>>>>>>>>>");

    }
}
