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
import tp2.ProducerAvro;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

public class ConsumerAvroDSLCumulVenteByDrug implements Runnable {

    private static Schema schema;
    private static Injection<GenericRecord, byte[]> recordInjection;
    private final KafkaConsumer<String, byte[]> consumer;
    public static final String CLIENT_ID = "testTopic";
    private HashMap<Integer, Double> cumulVenteByDrug;
    Properties props;
    private String topic;

    public ConsumerAvroDSLCumulVenteByDrug(String topic) {
        props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"Application_ConsumerAvroDSLCumulVenteByDrug");
        consumer = new KafkaConsumer<String, byte[]>(props);
        consumer.subscribe(Collections.singletonList(topic));
        cumulVenteByDrug = new HashMap<Integer, Double>();
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
                KStream<String,byte[]> sourceProcessor = builder.stream(topic,Consumed.with(stringSerdes,byteArray));
                
                sourceProcessor.foreach((x,y) -> {
                    GenericRecord record = recordInjection.invert(y).get();
                    int cip = (int)record.get("cip");
                    double prix = (double)record.get("prix");
                    if(cumulVenteByDrug.get(cip)==null){
                        cumulVenteByDrug.put(cip,prix);
                    }
                    else {
                        double vente = cumulVenteByDrug.get(cip);
                        prix = vente +prix;
                        cumulVenteByDrug.put(cip,prix);
                    }
                    System.out.println("Cip : " + cip+" | cumul :" + prix);
                });
                KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),props);
                kafkaStreams.start();

        } finally {
            consumer.close();
        }
    }

    public static void main(String[] args) {
        ConsumerAvroDSLCumulVenteByDrug consumerThread = new ConsumerAvroDSLCumulVenteByDrug("tp3");
        consumerThread.run();
    }

}
