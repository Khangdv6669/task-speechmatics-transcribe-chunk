'use strict'

const kafka = require('kafka-node'),
    config = require('config');

console.log(`Config loaded : ${JSON.stringify(config)}`);

const CHUNK_STATUS = {
  ERROR: 'ERROR',
  IGNORED: 'IGNORED',
  SUCCESS: 'SUCCESS'
};

console.log(`Initializing kafka client with Broker : ${config.kafka.brokers}
            , ChunkTopic : ${config.kafka.topics.chunkQueue}
            , InputTopic : ${config.kafka.topics.inputQueue}
            , ConsumerGroup : ${config.kafka.consumerGroupId}`);

const client = new kafka.KafkaClient(
    {
        kafkaHost: config.kafka.brokers
    }
);
const producer = new kafka.HighLevelProducer(client,
        {
            partitionerType: 3
        }
    );

function gracefulShutdown() {
    console.log('Initializing graceful shutdown');

    const cleanUpResource = [];
}

producer.on('error', err =>{
    console.log('Producer error :', err);
    gracefulShutdown();
});

producer.on('ready', () =>{
    console.log('Producer connected to kafka server');
    startQueueConsumption();
});

let consumerGroup;
function startQueueConsumption() {
    consumerGroup = new kafka.ConsumerGroup(
        {
            kafkaHost:config.kafka.brokers,
            groupId: config.kafka.consumerGroupId,
            protocol: ['roundrobin'],
            fromOffset: 'earliest',
            maxAsyncRequests: 10
        },
        config.kafka.topics.inputQueue
    );

    consumerGroup.on('error', err => {
        console.log('ConsumerGroup Error :', err);
    });

    consumerGroup.on('message', message =>{
        console.log('Message got :', message);
    });
};
