const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'payment-consumer',
    brokers: ['localhost:9092'], // Update if broker address is different
});

const consumer = kafka.consumer({ groupId: 'payment-group' });

const consumePayments = async () => {
    await consumer.connect();
    console.log('✅ Consumer connected');

    // Subscribe to the 'payment' topic
    await consumer.subscribe({ topic: 'payment', fromBeginning: true });
    console.log('📥 Subscribed to topic: payment');

    // Process each message
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const paymentData = message.value.toString(); // Get message as string
            try {
                const payment = JSON.parse(paymentData); // Parse JSON
                console.log(`📥 Received Payment:
        🆔 ID: ${payment.paymentId}
        💸 Amount: ${payment.amount}
        💳 Method: ${payment.paymentMethod}
        🎯 Reason: ${payment.reason}`);
            } catch (error) {
                console.error('❌ Error parsing message:', error);
            }
        },
    });
};

consumePayments().catch(console.error);
