package tp3;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import tp2.ProducerAvro;

import java.io.IOException;
import java.util.*;

public class ConsumerAvroByPartition implements Runnable {

    private static Schema schema;
    private static Injection<GenericRecord, byte[]> recordInjection;
    private final KafkaConsumer<String, byte[]> consumer;
    public static final String CLIENT_ID = "testTopic";
    Properties props;
    private int partition;
    private String topic;

    public ConsumerAvroByPartition(String topic, int partition) {
        props = new Properties();
        props.put("bootstrap.servers","localhost:9092");


        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"Application_id");
        consumer = new KafkaConsumer<String, byte[]>(props);
        this.partition=partition;
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

            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            Collection<TopicPartition> partitions = new ArrayList<>();
            int count=0;
            if (partitionInfos != null) {
                TopicPartition partition0 = new TopicPartition(topic, partition);
                consumer.assign(Arrays.asList(partition0));
                ConsumerRecords<String, byte[]> records = consumer.poll(1000);
                for (ConsumerRecord<String, byte[]> record: records) {
                    GenericRecord recordDrug = recordInjection.invert(record.value()).get();
                    int idPharmacie = (int)recordDrug.get("idpharma");
                    double prix = (double)recordDrug.get("prix");
                    System.out.println("Consumer : "+partition+" | topic : "+record.topic()+" | partition : "+record.partition()+" | idPharma : "+idPharmacie+"| prix : "+prix);
                    count++;
                }
            }
            System.out.println("Total partition "+partition+" : "+count );
            consumer.commitSync();



        } finally {
            consumer.close();
        }


    }

    public static void main(String[] args) {
        ConsumerAvroByPartition consumerThread = new ConsumerAvroByPartition("tp3Partition",0);
        consumerThread.run();
        System.out.println("------------------------------------");
        ConsumerAvroByPartition consumerThread2 = new ConsumerAvroByPartition("tp3Partition",1);
        consumerThread2.run();
        System.out.println("------------------------------------");
        ConsumerAvroByPartition consumerThread3 = new ConsumerAvroByPartition("tp3Partition",2);
        consumerThread3.run();
        System.out.println("------------------------------------");

    }

}
