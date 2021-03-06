package tp3;

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
import org.apache.kafka.streams.kstream.Produced;
import tp2.ProducerAvro;


import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

public class ConsumerAvroDSLToConsumerByPartition implements Runnable {

    private static Schema schema;
    private static Injection<GenericRecord, byte[]> recordInjection;
    private final KafkaConsumer<String, byte[]> consumer;
    public static final String CLIENT_ID = "testTopic";
    private HashMap<Integer, Double> CumulVenteByPharmacie;
    Properties props;
    private String topic;

    public ConsumerAvroDSLToConsumerByPartition(String topic) {
        props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"Application_ConsumerAvroDSLToConsumerByPartition");
        consumer = new KafkaConsumer<String, byte[]>(props);
        consumer.subscribe(Collections.singletonList(topic));
        CumulVenteByPharmacie = new HashMap<Integer, Double>();
        this.topic=topic;



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
            KStream<String,byte[]> sourceProcessor = builder.stream("tp3", Consumed.with(stringSerdes,byteArray));


            StreamPartionner key = new StreamPartionner();


            sourceProcessor.to("tp3Partition", Produced.with(stringSerdes, byteArray).withStreamPartitioner(key));
            KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),props);
            kafkaStreams.start();


        } finally {
            consumer.close();
        }

    }


    public static void main(String[] args) {
        ConsumerAvroDSLToConsumerByPartition consumerThread = new ConsumerAvroDSLToConsumerByPartition("tp3");
        consumerThread.run();
    }

}