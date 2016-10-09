import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by melvin on 9/10/16.
 */
public class KafkaProducerTest {

    public static void main(String args[]){

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        Integer partition =  0; //this is the partition where messages will be sent

        for(int i = 0; i < 1000000; i++) {
            //Option 1. Produce to all partiions
            producer.send(new ProducerRecord<String, String>("topic888", Integer.toString(i), "test-" + Integer.toString(i)));

            //Option 2. Produce to a specific partition
            //producer.send(new ProducerRecord<String, String>("topic888", partition, Integer.toString(i), "test-" + Integer.toString(i)));
        }
        producer.close();

    }
}
