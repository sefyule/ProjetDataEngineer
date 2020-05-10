package tp2;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class ConsumerAvro implements Runnable {

    private static Schema schema;
    private static Injection<GenericRecord, byte[]> recordInjection;
    private final KafkaConsumer<String, byte[]> consumer;
    public static final java.lang.String CLIENT_ID = "testTopic";
    Properties props;

    public ConsumerAvro(String topic) {
        props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
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

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, byte[]> record : records) {
                    scala.util.Try<GenericRecord> genericRecord = recordInjection.invert(record.value());
                    System.out.println(genericRecord.get());
                    //System.out.println(genericRecord.get().get("nom"));

                }
            }
        } finally {
            consumer.close();
        }

    }
    public static void main(String[] args) {
        ConsumerAvro consumerThread = new ConsumerAvro("topicTp2");
        consumerThread.run();
    }

}
