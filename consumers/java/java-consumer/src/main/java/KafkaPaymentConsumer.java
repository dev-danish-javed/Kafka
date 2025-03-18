import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaPaymentConsumer {

    public static void main(String[] args) {
        // Kafka Consumer Properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka broker
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "java-payment-group"); // Consumer group
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Read messages from the beginning

        // Create Kafka Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("payment"));
        System.out.println("✅ Consumer subscribed to topic: payment");

        ObjectMapper objectMapper = new ObjectMapper(); // To parse JSON

        // Poll for new messages
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        String paymentJson = record.value(); // Get message as JSON
                        Payment payment = objectMapper.readValue(paymentJson, Payment.class); // Deserialize to Payment
                        System.out.printf("📥 Received Payment: %s%n", payment.toString());
                    } catch (Exception e) {
                        System.err.println(" Error parsing message: " + e.getMessage());
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}
