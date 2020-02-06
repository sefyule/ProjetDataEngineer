import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MessageProducer {

    public static void main (String args[]) {

        try {

            ObjectMapper mapper = new ObjectMapper().setVisibility(JsonMethod.FIELD, JsonAutoDetect.Visibility.ANY);
            Properties props = new Properties();
            props.put("bootstrap.servers","localhost:9092");
            props.put("key.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
            Faker faker = new Faker();
            List<Message> list= new ArrayList<Message>();
            for(int i=0; i<100 ; i++ ){
                Message message = new Message();
                String jsonMessage = mapper.writeValueAsString(message);
                System.out.println(jsonMessage);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("test" , jsonMessage);
                list.add(message);
                producer.send(record);
            }
            producer.close();
            mapper.writeValue(new File("Messages.json"), list);
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }


        System.out.println("Fin");
    }
}
