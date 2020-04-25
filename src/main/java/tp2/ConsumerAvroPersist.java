package tp2;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

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
        props.put("bootstrap.servers","localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"Application_id");
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

            Serde<String> stringSerdes = Serdes.String();
            Serde<byte[]> byteArray = new Serdes.ByteArraySerde();
            StreamsBuilder builder = new StreamsBuilder();
            Connection connection = null;

            try{
                connection = DriverManager.getConnection("jdbc:postgresql://sqletud.u-pem.fr/ychekiri_db", "ychekiri", "01/02/1961");
                Connection tmp = connection;

                System.out.println(connection.getClientInfo());

                KStream<String, byte[]> sourceProcessor = builder.stream("tp2", Consumed.with(stringSerdes, byteArray));

                sourceProcessor.foreach((x, y) -> {
                    try {
                        queryInsert(tmp, y);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }

                });
                KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
                kafkaStreams.start();

            } catch (SQLException e) {
                e.printStackTrace();
            }
            finally {
                connection.close();
            }
        } catch (IllegalStateException e) {
            e.printStackTrace();
        } catch (StreamsException | SQLException e) {
            e.printStackTrace();
        }
    }

    private void queryInsert(Connection connection, byte[] y) throws SQLException {
        GenericRecord genericRecord = recordInjection.invert(y).get();
        String nom =  genericRecord.get("nom").toString();
        String prenom =  genericRecord.get("prenom").toString();
        int cip = Integer.parseInt(genericRecord.get("cip").toString());
        double prix = Double.parseDouble(genericRecord.get("prix").toString());
        int id_pharma = Integer.parseInt(genericRecord.get("idpharma").toString());
        try {
            PreparedStatement st = connection.prepareStatement("INSERT INTO transaction (nom, prenom, cip, prix, id_pharma) VALUES (?, ?, ?, ?, ?);");
            st.setString(1, nom);
            st.setString(2, prenom);
            st.setInt(3, cip);
            st.setDouble(4, prix);
            st.setInt(5, id_pharma);
            st.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ConsumerAvroPersist consumerThread = new ConsumerAvroPersist("tp2");
        consumerThread.run();
    }
}
