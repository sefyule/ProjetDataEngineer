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

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class ConsumerGetMessageJoinDrugPharmacy implements Runnable {

    private static Schema drugSchema;
    private static Schema pharmacySchema;
    private static Injection<GenericRecord, byte[]> recordInjectionDrug;
    private static Injection<GenericRecord, byte[]> recordInjectionPharmacy;
    private final KafkaConsumer<String, byte[]> consumer;
    public static final String CLIENT_ID = "testTopic";
    Properties props;
    private String topic;

    public ConsumerGetMessageJoinDrugPharmacy(String topic) {
        props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"Application_ConsumerGetMessageJoinDrugPharmacy");
        consumer = new KafkaConsumer<String, byte[]>(props);
        consumer.subscribe(Collections.singletonList(topic));
        this.topic=topic;
    }
    @Override
    public void run() {
        try{
            Schema.Parser parser = new Schema.Parser();
            try {
                drugSchema = parser.parse(ConsumerGetMessageJoinDrugPharmacy.class.getResourceAsStream("drugtxn.avsc"));
                pharmacySchema = parser.parse(ConsumerGetMessageJoinDrugPharmacy.class.getResourceAsStream("pharmacy.avsc"));
            } catch (IOException e) {
                e.printStackTrace();
            }
            recordInjectionDrug = GenericAvroCodecs.toBinary(drugSchema);
            recordInjectionPharmacy = GenericAvroCodecs.toBinary(pharmacySchema);

            Serde<String> stringSerdes = Serdes.String();
            Serde<byte[]> byteArray = new Serdes.ByteArraySerde();
            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, byte[]> drugs = builder.stream("tp3", Consumed.with(stringSerdes, byteArray));
            KStream<String, byte[]> pharmacys = builder.stream("topicpharmacy", Consumed.with(stringSerdes, byteArray));


            drugs.selectKey((k, v) -> ""+ recordInjectionDrug.invert(v).get().get("idpharma")).to("topicJoinDrugs", Produced.with(stringSerdes, byteArray));
            pharmacys.selectKey((k, v) -> ""+ recordInjectionPharmacy.invert(v).get().get("id")).to("topicJoinPharmacys", Produced.with(stringSerdes, byteArray));
            KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),props);
            kafkaStreams.start();



        } finally {
            consumer.close();
        }

    }
    public static void main(String[] args) {
        ConsumerGetMessageJoinDrugPharmacy consumerThread = new ConsumerGetMessageJoinDrugPharmacy("tp3");
        consumerThread.run();
    }



}
