const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'my-consumer',
    brokers: ['localhost:9092'], // Ensure this matches your Kafka setup
});

const consumer = kafka.consumer({ groupId: 'test-group' });

const consumeMessages = async () => {
    await consumer.connect();
    console.log('✅ Consumer connected');

    await consumer.subscribe({ topic: 'test', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`📥 Received: ${message.value.toString()} (Partition: ${partition})`);
        },
    });
};

consumeMessages().catch(console.error);
