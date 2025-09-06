import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import org.slf4j.Logger;

public class Hellokafka {
    private static final Logger logger = LoggerFactory.getLogger(Hellokafka.class.getName());
    public static void main(String[] args) {
        logger.info("Hello, Kafka!");
        Properties properties = new Properties();
        //connect to localhost
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);;
        //create a producer record
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("first_topic","hello world" );

        //send data - asynchronous
        producer.send(producerRecord);

        //flush data and close producer
        producer.flush();
        producer.close();
        logger.info("Message sent successfully");
    }
}
