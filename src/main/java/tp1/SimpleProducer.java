package tp1;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SimpleProducer {

	public static void main (String args[]) {


		Properties props = new Properties();
		props.put("bootstrap.servers","localhost:9092");
		props.put("key.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer <String, String> producer = new KafkaProducer<String, String>(props);
		Faker faker = new Faker();
		List<String> list= new ArrayList<String>();
		for(int i=0; i<100 ; i++ ){
			String artiste = faker.artist().name();
			ProducerRecord <String, String> record = new ProducerRecord<String, String>("test" , artiste);
			list.add(artiste);
			producer.send(record);
		}
		producer.close();

		ObjectMapper mapper = new ObjectMapper();
		try {
			//Convert object to JSON string and save into file directly
			mapper.writeValue(new File("artiste.json"), list);



		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}


		System.out.println("Fin");
	}
    /* Debut de question 7 :
	try (
    Connection connection = DriverManager.getConnection("jdbc:postgresql://sqletud.u-pem.fr/ychekiri_db", "ychekiri", "01/02/1961")) {
        Statement statement = ((Connection) connection).createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM ciscipdenorm");


        while (resultSet.next()) {
            if (count < 1000) {
                resultSet.getString("cip7");*/

}
