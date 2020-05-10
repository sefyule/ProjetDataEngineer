package tp3;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ConsumerJoinDrugPharmacy implements Runnable {

    private static Schema drugSchema;
    private static Schema pharmacySchema;
    private static Schema resultSchema;

    private static Injection<GenericRecord, byte[]> recordInjectionDrug;
    private static Injection<GenericRecord, byte[]> recordInjectionPharmacy;
    private static Injection<GenericRecord, byte[]> recordInjectionResultJoin;

    private final KafkaConsumer<String, byte[]> consumer;
    public static final String CLIENT_ID = "testTopic";
    Properties props;
    private String topic;

    public ConsumerJoinDrugPharmacy(String topic) {
        props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"Application_ConsumerJoinDrugPharmacy");
        consumer = new KafkaConsumer<String, byte[]>(props);
        consumer.subscribe(Collections.singletonList(topic));
        this.topic=topic;
    }
    @Override
    public void run() {
        try{
            Schema.Parser parser = new Schema.Parser();
            try {
                drugSchema = parser.parse(ConsumerJoinDrugPharmacy.class.getResourceAsStream("drugtxn.avsc"));
                pharmacySchema = parser.parse(ConsumerJoinDrugPharmacy.class.getResourceAsStream("pharmacy.avsc"));
                resultSchema = parser.parse(ConsumerJoinDrugPharmacy.class.getResourceAsStream("drugJoinPharmacy.avsc"));
            } catch (IOException e) {
                e.printStackTrace();
            }
            recordInjectionDrug = GenericAvroCodecs.toBinary(drugSchema);
            recordInjectionPharmacy = GenericAvroCodecs.toBinary(pharmacySchema);
            recordInjectionResultJoin = GenericAvroCodecs.toBinary(resultSchema);


            Serde<String> stringSerdes = Serdes.String();
            Serde<byte[]> byteArray = new Serdes.ByteArraySerde();
            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, byte[]> drugs = builder.stream("topicJoinDrugs", Consumed.with(stringSerdes, byteArray));
            KStream<String, byte[]> pharmacys = builder.stream("topicJoinPharmacys", Consumed.with(stringSerdes, byteArray));


            KStream<String,byte[]> drugsJoinPharmacys = drugs.join(pharmacys, (leftValue, rightValue) ->  JoinLeftRightValue(leftValue, recordInjectionDrug, rightValue, recordInjectionPharmacy),
                    JoinWindows.of(TimeUnit.MINUTES.toMillis(1000)),
                    Joined.with(
                            stringSerdes,
                            byteArray,
                            byteArray)
            );
            drugsJoinPharmacys.to("resultat", Produced.with(stringSerdes, byteArray));

            KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),props);
            kafkaStreams.start();



        } finally {
            consumer.close();
        }

    }

    public static byte[] JoinLeftRightValue(byte[] leftValue, Injection<GenericRecord, byte[]> injectionLeft, byte[] rightValue, Injection<GenericRecord, byte[]> injectionRight) {
        GenericRecord recordTransaction = injectionLeft.invert(leftValue).get();
        GenericRecord recordRegion = injectionRight.invert(rightValue).get();
        GenericData.Record genericRecord = new GenericData.Record(resultSchema);

        genericRecord.put("nom", recordTransaction.get("nom"));
        genericRecord.put("prenom", recordTransaction.get("prenom"));
        genericRecord.put("cip", recordTransaction.get("cip"));
        genericRecord.put("prix", recordTransaction.get("prix"));
        genericRecord.put("idpharma", recordTransaction.get("idpharma"));
        genericRecord.put("region", recordRegion.get("region"));


        System.out.println("Region : " +recordRegion.get("region") );
        System.out.println("nom : " +recordRegion.get("nom") );

        return recordInjectionResultJoin.apply(genericRecord);

    }

    public static void main(String[] args) {
        ConsumerJoinDrugPharmacy consumerThread = new ConsumerJoinDrugPharmacy("tp3");
        consumerThread.run();
    }

}


