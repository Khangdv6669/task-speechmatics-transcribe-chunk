'use strict'

const kafka = require('kafka-node'),
    _ = require('lodash'),
    process = require('process'),
    path = require('path'),
    config = require('config'),
    SpeechmaticsClient = require('./speechmatics-client');

console.log(`Config loaded : ${JSON.stringify(config)}`);

const CHUNK_STATUS = {
  ERROR: 'ERROR',
  IGNORED: 'IGNORED',
  SUCCESS: 'SUCCESS'
};

// Start Idle timer
console.log(`Started with config : ${config.enIfIdleSecs}`);
let idleTimer = setTimeout(gracefulShutdown, config.enIfIdleSecs * 1000);

// config graceful shutdown handlers
process.on('SIGINT',gracefulShutdown);
process.on('SIGTERM',gracefulShutdown);

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

        const {value} = message;
        console.log(`Value got: ${value}`);
        let event;
        try {
            event = JSON.parse(value);
            console.log('Event got: ', event);
        } catch (e) {
            console.log('Failed to json unmarshal:' + value);
            return;
        }

        // validate kafka message
        const error = validateEvent(event);
        if (!_.isEmpty(error)) {
            sendChunkProcessed(event, CHUNK_STATUS.IGNORED, warnings.join(','), t);
            return;
        }

        // try to createjob test
        var opts = {
            assetURI: event.cacheURI,
            language: 'en-US'
        };
        const SpeechmaticsClient = new SpeechmaticsClient(config['speechmatics'].userIid, config['speechmatics'].apiKey,opts);
        SpeechmaticsClient.createJob(opts, function createJobCallback(err, resp) {
            if (err) {
                return callback(err);
            }
            console.log(resp)
            //callback(null, resp);
        });
    });
};


/**
 * Validata if kafka message is of right type and mimeType
 */

function validateEvent(event) {
    let errs = [];
    if (event.type !== 'media_chunk') {
        errs.push(`Engine Type was not media_chunk`);
    }else {
        console.log(`It was chunk engine`);
    }
    console.log(event.type);
    console.log(event.cacheURI);
    let fileType = path.extname(event.cacheURI).substring(1).toLowerCase();
    console.log(`File type :${fileType} and supported  :${config.speechmatics.supportedInputs[fileType]}`);
    if (!config.speechmatics.supportedInputs[fileType]) {
        errs.push(`File type was not supprot`);
    }
    return errs;
}

/**
 * Send chunk process status
 */

function sendChunkProcessed(event, status, mess, processTime) {
    if(mess){}

    let t = processTime;
    if (processTime === undefined || processTime === null) {
        t = process.hrtime();
    }
}


