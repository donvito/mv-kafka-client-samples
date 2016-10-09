import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by melvin on 9/10/16.
 */
public class KafkaConsumerTest {

    public static void main(String args[]){

        String topic = "topic888";
        List topics = Arrays.asList(topic);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092" );
        props.put("group.id", "grp1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //Option 1. Uncomment below if you want to consume from all partitions by default
        //consumer.subscribe(topics);

        //Option 2. Uncomment below if you want to consume messages from a specific partition
        TopicPartition partition0 = new TopicPartition(topic, 2);
        consumer.assign(Arrays.asList(partition0));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value() + "\n");
            }
        }

    }
}
