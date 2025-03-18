import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class Producer {

    public static void main(String[] args) {
        // Kafka Producer Properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka broker
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Create Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Send 5 sample messages to 'test' topic
        for (int i = 1; i <= 5; i++) {
            String message = "Hello Kafka from Java " + i;
            ProducerRecord<String, String> record = new ProducerRecord<>("test", message);

            try {
                Future<RecordMetadata> future = producer.send(record);
                RecordMetadata metadata = future.get(); // Synchronous send (optional)
                System.out.printf("📩 Sent: %s to Partition: %d, Offset: %d%n",
                        message, metadata.partition(), metadata.offset());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Close producer
        producer.close();
        System.out.println("✅ Producer disconnected");
    }
}
