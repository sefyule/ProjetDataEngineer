package tp2;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;


import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class ConsumerAvroDSL implements Runnable {

    private static Schema schema;
    private static Injection<GenericRecord, byte[]> recordInjection;
    private final KafkaConsumer<String, byte[]> consumer;
    public static final String CLIENT_ID = "testTopic";
    Properties props;

    public ConsumerAvroDSL(String topic) {
        props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"Application_ConsumerAvroDSL");
        consumer = new KafkaConsumer<String, byte[]>(props);
        consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void run() {
        try{
            Schema.Parser parser = new Schema.Parser();
            try {
                schema = parser.parse(ProducerAvro.class.getResourceAsStream("drugtxn.avsc"));
            } catch (IOException e) {
                e.printStackTrace();
            }
            recordInjection = GenericAvroCodecs.toBinary(schema);

                Serde<String> stringSerdes = Serdes.String();
                Serde<byte[]> byteArray = new Serdes.ByteArraySerde();
                StreamsBuilder builder = new StreamsBuilder();
                KStream<String,byte[]> sourceProcessor = builder.stream("topicTp2",Consumed.with(stringSerdes,byteArray));
                sourceProcessor.foreach((x,y) -> {
                    GenericRecord record = recordInjection.invert(y).get();
                    System.out.println(record.get("nom"));
                });
                KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),props);
                kafkaStreams.start();

        } finally {
            consumer.close();
        }

    }

    public static void main(String[] args) {
        ConsumerAvroDSL consumerThread = new ConsumerAvroDSL("topicTp2");
        consumerThread.run();
    }

}
