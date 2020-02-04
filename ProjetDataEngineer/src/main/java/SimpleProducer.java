import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleProducer {

 	public static void main (String args[]) {
		Properties props = new Properties();
		props.put("bootstrap.servers","localhost:9092");
		props.put("key.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer <String, String> producer = new KafkaProducer<String, String>(props);
		for(int i=0; i<100 ; i++ ){
			ProducerRecord <String, String> record = new ProducerRecord<String, String>("test" , "abc");
			producer.send(record);
		}
		producer.close();
		}

}
