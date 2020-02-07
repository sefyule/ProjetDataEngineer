package tp1;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Producer {

    public static void main (String args[]) {

        try {

        ObjectMapper mapper = new ObjectMapper();
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("key.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer <String, String> producer = new KafkaProducer<String, String>(props);
        Faker faker = new Faker();
        List<Personne> list= new ArrayList<Personne>();
        for(int i=0; i<100 ; i++ ){
            Personne personne = new Personne();
            String jsonPersonne = mapper.writeValueAsString(personne);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("testTopic" , jsonPersonne);
            list.add(personne);
            producer.send(record);
        }
        producer.close();
        mapper.writeValue(new File("Personnes.json"), list);
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        System.out.println("Fin");
    }
}
