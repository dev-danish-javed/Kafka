import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class PaymentKafkaProducer {

    public static void main(String[] args) {
        // Kafka Producer Properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka broker
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Create Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper objectMapper = new ObjectMapper();

        // Create payment data
        Payment payment1 = new Payment("P123", 999.50f, "Credit Card", "Online Purchase");
        Payment payment2 = new Payment("P124", 499.99f, "UPI", "Subscription");
        Payment payment3 = new Payment("P125", 1500.75f, "Net Banking", "EMI Payment");

        // Send payment data to Kafka
        sendPayment(producer, objectMapper, payment1);
        sendPayment(producer, objectMapper, payment2);
        sendPayment(producer, objectMapper, payment3);

        // Close producer
        producer.close();
        System.out.println("✅ Producer disconnected");
    }

    // Method to send Payment object
    private static void sendPayment(KafkaProducer<String, String> producer, ObjectMapper objectMapper, Payment payment) {
        try {
            String paymentJson = objectMapper.writeValueAsString(payment); // Serialize Payment to JSON
            ProducerRecord<String, String> record = new ProducerRecord<>("payment", payment.getPaymentId(), paymentJson);
            producer.send(record);
            System.out.printf("📩 Sent Payment: %s%n", paymentJson);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
