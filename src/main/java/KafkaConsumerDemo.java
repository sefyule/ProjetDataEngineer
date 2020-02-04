import org.apache.kafka.clients.consumer.Consumer;

public class KafkaConsumerDemo {
    public static void main(String[] args) {
        KafkaSampleConsumer consumerThread = new KafkaSampleConsumer("test");
        consumerThread.start();
    }
}
