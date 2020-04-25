package tp2;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import tp1.Message;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ProducerAvro {

    private static Schema schema;
    private static Injection<GenericRecord, byte[]> recordInjection;

    public static void main (String args[]) {

        try {

            ObjectMapper mapper = new ObjectMapper().setVisibility(JsonMethod.FIELD, JsonAutoDetect.Visibility.ANY);
            Properties props = new Properties();
            props.put("bootstrap.servers","localhost:9092");
            props.put("key.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer" , "org.apache.kafka.common.serialization.ByteArraySerializer");
            KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(ProducerAvro.class.getResourceAsStream("drugtxn.avsc"));
            recordInjection = GenericAvroCodecs.toBinary(schema);


            List<Message> list= new ArrayList<Message>();
            for(int i=0; i<100 ; i++ ){
                Message message = new Message();
                GenericData.Record genericRecord = new GenericData.Record(schema);
                genericRecord.put("nom", message.getNom());
                genericRecord.put("prenom", message.getPrenom());
                genericRecord.put("cip", message.getCip());
                genericRecord.put("prix", message.getPrix());
                genericRecord.put("idpharma", message.getIdpharma());
                ProducerRecord<String, byte[]> record = new ProducerRecord<>("tp2" , recordInjection.apply(genericRecord));
                list.add(message);
                producer.send(record);
            }
            producer.close();
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
