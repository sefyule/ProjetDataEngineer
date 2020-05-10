package tp4;

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
import tp2.ProducerAvro;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Grafana {

    private static Schema schema;
    private static Injection<GenericRecord, byte[]> recordInjection;
    private static Connection connexion;

    public Grafana() {
        final String URL = "jdbc:mysql://remotemysql.com:3306/CgOJWRXTYv";
        final String LOGIN = "CgOJWRXTYv";
        final String PASSWORD = "jjothkqy4a";

        try(Connection cnx = DriverManager.getConnection(URL, LOGIN, PASSWORD)) {
            this.connexion =cnx;
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        new Grafana();

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
            while(true){
                Message message = new Message();
                GenericData.Record genericRecord = new GenericData.Record(schema);
                genericRecord.put("cip", message.getCip());
                genericRecord.put("prix", message.getPrix());
                ProducerRecord<String, byte[]> record = new ProducerRecord<>("grafanaTopic" , recordInjection.apply(genericRecord));
                list.add(message);
                Statement statement = null;
                statement = ((Connection) connexion).createStatement();
                String query = "INSERT INTO `cumulDrugs`(`cip`, `prix`) VALUES (\""+message.getCip()+"\","+message.getPrix()+")";
                int resultSet = statement.executeUpdate(query);
                Thread.sleep(1500);
                producer.send(record);
            }
           // producer.close();
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InterruptedException e){
            e.printStackTrace();
        }
    }



}
