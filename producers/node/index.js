const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'my-producer',
    brokers: ['localhost:9092'], // Change if using a different Kafka host
});

const producer = kafka.producer();

const produceMessage = async () => {
    await producer.connect();
    console.log('✅ Producer connected');

    for (let i = 1; i <= 5; i++) {
        const message = `Hello Kafka Danish ${i}`;
        await producer.send({
            topic: 'test',
            messages: [{ value: message }],
        });
        console.log(`📩 Sent: ${message}`);
    }

    await producer.disconnect();
    console.log('❌ Producer disconnected');
};

produceMessage().catch(console.error);
