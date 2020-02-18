package tp2;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.IOException;
import java.sql.*;
import java.util.Collections;
import java.util.Properties;

public class ConsumerAvroPersist implements Runnable {

    private static Schema schema;
    private static Injection<GenericRecord, byte[]> recordInjection;
    private final KafkaConsumer<String, byte[]> consumer;
    public static final String CLIENT_ID = "testTopic";
    Properties props;

    public ConsumerAvroPersist(String topic) {
        props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        consumer = new KafkaConsumer<String, byte[]>(props);
        consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void run() {
        try {
            Schema.Parser parser = new Schema.Parser();
            try {
                schema = parser.parse(ProducerAvro.class.getResourceAsStream("drugtxn.avsc"));
            } catch (IOException e) {
                e.printStackTrace();
            }
            recordInjection = GenericAvroCodecs.toBinary(schema);

            while (true) {
                try (Connection connection = DriverManager.getConnection("jdbc:postgresql://sqletud.u-pem.fr/ychekiri_db", "ychekiri", "01/02/1961")) {
                    Statement statement = null;
                    try {
                        statement = ((Connection) connection).createStatement();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    try {
                        ConsumerRecords<String, byte[]> records = consumer.poll(Long.MAX_VALUE);
                        for (ConsumerRecord<String, byte[]> record : records) {
                            scala.util.Try<GenericRecord> genericRecord = recordInjection.invert(record.value());
                            String nom = (String) genericRecord.get().get("nom");
                            String prenom = (String) genericRecord.get().get("prenom");
                            int cip = (int) genericRecord.get().get("cip");
                            double prix = (double) genericRecord.get().get("prix");
                            int id_pharma = (int) genericRecord.get().get("id_pharma");

                            /* A tester ! Surement 2 eme cas */
                            ResultSet resultSet = statement.executeQuery("INSERT INTO transaction (nom, prenom, cip, prix, id_pharma) VALUES (nom, prenom, cip, prix, id_pharma);");

                            /*PreparedStatement st = connection.prepareStatement("INSERT INTO transaction (nom, prenom, cip, prix, id_pharma) VALUES (?, ?, ?, ?, ?)");
                            st.setString(1, nom);
                            st.setString(2, prenom);
                            st.setInt(3, cip);
                            st.setDouble(4, prix);
                            st.setInt(4, id_pharma);
                            st.executeUpdate();
                            st.close();*/

                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        } finally {
            consumer.close();
        }

    }

    public static void main(String[] args) {
        ConsumerAvroPersist consumerThread = new ConsumerAvroPersist("tp2");
        consumerThread.run();
    }


}