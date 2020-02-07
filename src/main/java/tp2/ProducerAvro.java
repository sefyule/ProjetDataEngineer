package tp2;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.twitter4j.Twitter4jStatusClient;
import com.twitter.hbc.twitter4j.handler.StatusStreamHandler;
import com.twitter.hbc.twitter4j.message.DisconnectMessage;
import com.twitter.hbc.twitter4j.message.StallWarningMessage;

import tp1.Message;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.User;

public class ProducerAvro {


    // Kafka Producer - note that Kafka Producers are Thread Safe and that sharing a Producer instance
    // across threads is generally faster than having multiple Producer instances
    private static KafkaProducer<String, byte[]> producer;

    // Note that for a Production Deployment, do not hard-code your Twitter Application Authentication Keys
    // Instead, derive from a Configuration File or Context
    private static final String CONSUMER_KEY = "<Your Twitter Application Consumer Key>";
    private static final String CONSUMER_SECRET = "<Your Twitter Application Consumer Secret>";
    private static final String ACCESS_TOKEN = "<Your Twitter Application Access Token>";
    private static final String ACCESS_TOKEN_SECRET = "<Your Twitter Application Access Token Secret>";
    private static final String KAFKA_TOPIC = "twitter";

    // Avro Schema to use to serialise messages to the Kafka Topic
    // For the full list of Tweet fields, please refer to
    // https://dev.twitter.com/overview/api/tweets
    private static Schema schema;
    private static Injection<GenericRecord, byte[]> recordInjection;

    /**
     * Wrap a Twitter4j Client around a Hosebird Client using a custom Status Listener
     * and an Executor Service to spawn threads to parse the messages received
     * @param kafkaBroker
     * @throws InterruptedException
     */

    public static void run(String kafkaBroker) throws InterruptedException {



        // Kafka Producer Properties
        Properties producerProperties = new Properties();

        // Bootstrapping
        producerProperties.put("bootstrap.servers", kafkaBroker);

        // Serializer Class for Keys
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Serializer Class for Values
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        // When a Produce Request is considered completed
        producerProperties.put("request.required.acks", "1");

        // Create the Kafka Producer
        producer = new KafkaProducer<>(producerProperties);

        // Twitter Connection and Filtering Properties
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<String>(100000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.stallWarnings(false);
        endpoint.trackTerms(Lists.newArrayList("brexit", "#vote2017"));
        Authentication authentication = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);

        // Build a Twitter Hosebird Client
        ClientBuilder hosebirdClientBuilder = new ClientBuilder()
                .name("HyperLearning AI Knowledgebase Twitter Hosebird Client")
                .hosts(Constants.STREAM_HOST)
                .authentication(authentication)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(messageQueue));
        BasicClient hosebirdClient = hosebirdClientBuilder.build();

        // Create an Executor Service to spawn threads to parse the messages
        // Runnables are submitted to the Executor Service to process the Message Queue
        int numberProcessingThreads = 2;
        ExecutorService service = Executors.newFixedThreadPool(numberProcessingThreads);

        // Wrap a Twitter4j Client around the Hosebird Client using a custom Status Listener
        Twitter4jStatusClient twitter4jClient = new Twitter4jStatusClient(
                hosebirdClient, messageQueue, Lists.newArrayList(statusListener), service);

        // Connect to the Twitter Streaming API
        twitter4jClient.connect();

        // Twitter4jStatusClient.process must be called for every Message Processing Thread to be spawned
        for (int threads = 0; threads < numberProcessingThreads; threads++) {
            twitter4jClient.process();
        }

        // Run the Producer for 60 seconds for DEV purposes
        // Note that this is NOT a graceful exit
        Thread.sleep(60000);
        producer.close();
        hosebirdClient.stop();

    }


    private static StatusListener statusListener = new StatusStreamHandler() {

        @Override
        public void onStatus(Status status) {

            // Convert the Status object into an Avro Record for serialising and publishing to the Kafka Topic
            GenericData.Record avroRecord = createRecord(status);
            byte[] avroRecordBytes = recordInjection.apply(avroRecord);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>("test", avroRecordBytes);

            // Send the Message to Kafka
            producer.send(record);

        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}

        @Override
        public void onTrackLimitationNotice(int limit) {}

        @Override
        public void onScrubGeo(long user, long upToStatus) {}

        @Override
        public void onStallWarning(StallWarning warning) {}

        @Override
        public void onException(Exception e) {}

        @Override
        public void onDisconnectMessage(DisconnectMessage message) {}

        @Override
        public void onStallWarningMessage(StallWarningMessage warning) {}

        @Override
        public void onUnknownMessageType(String s) {}

    };


    private static GenericData.Record createRecord(Status status) {

        User user = status.getUser();
        GenericData.Record doc = new GenericData.Record(schema);

        try {
            Message message = new Message();
            doc.put("nom", message.getNom());
            doc.put("prenom", message.getPrenom());
            doc.put("cip", message.getCip());
            doc.put("prix", message.getPrix());
            doc.put("idpharma", message.getIdpharma());

        }catch (Exception e){

        }

        return doc;

    }

    public static void main(String[] args) {



        try {

            // Create the Avro Schema
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(ProducerAvro.class.getResourceAsStream("drugtxn.avsc"));

            recordInjection = GenericAvroCodecs.toBinary(schema);
            GenericData.Record record = new GenericData.Record(schema);

            Message message = new Message();
            record.put("nom", message.getNom());
            byte[] b = recordInjection.apply(record);



            ProducerAvro.run(args[0]);

        } catch (Exception e) {

            System.out.println(e);

        }

    }

}