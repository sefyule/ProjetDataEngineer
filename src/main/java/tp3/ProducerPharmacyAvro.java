package tp3;

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

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ProducerPharmacyAvro {
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
            schema = parser.parse(ProducerPharmacyAvro.class.getResourceAsStream("pharmacy.avsc"));
            recordInjection = GenericAvroCodecs.toBinary(schema);

            List<Pharmacy> list= new ArrayList<Pharmacy>();
            int count = 0;
            for(int i=0; i<100 ; i++ ){
                Pharmacy pharmacy = new Pharmacy();
                GenericData.Record genericRecord = new GenericData.Record(schema);
                genericRecord.put("id",pharmacy.getId());
                genericRecord.put("nom", pharmacy.getNom());
                genericRecord.put("adresse", pharmacy.getAdresse());
                genericRecord.put("depart", pharmacy.getDepart());
                genericRecord.put("region", pharmacy.getRegion());
                ProducerRecord<String, byte[]> record = new ProducerRecord<>("topicpharmacy" , recordInjection.apply(genericRecord));
                list.add(pharmacy);
                producer.send(record);
                count++;
            }
            System.out.println("Total Produce : "+count);
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
