import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.codehaus.jackson.map.deser.std.StdDeserializer;

import java.util.*;

public class KafkaSampleConsumer extends ShutdownableThread {
    //private final static String TOPIC = "testTopic";
    private final KafkaConsumer<String, String> consumer;
    private final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final String CLIENT_ID = "SampleConsumer";
    Properties props;
    private final String topic;

    public KafkaSampleConsumer(String topic) {
        super("KafkaConsumerExample", false);
        props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "round-robin");
        consumer = new KafkaConsumer<String, String>(props);
        this.topic = topic;
    }

    @Override
    public void doWork() {
        System.out.println("TOPIC :"+ this.topic);
        consumer.subscribe(String.valueOf(Arrays.asList(props.getProperty(this.topic))));
        System.out.println("Consumer :"+ consumer.poll(1000));
        while (true) {
            ConsumerRecords<String, String> consumerRecords = (ConsumerRecords<String, String>) consumer.poll(1000);
            ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) consumer.poll(100);
            if(records!=null) System.out.println(records);

        }
    }
    @Override
    public String name() {
        return null;
    }

    @Override
    public boolean isInterruptible() {
        return false;
    }

}
