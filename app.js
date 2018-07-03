'use strict'

const kafka = require('kafka-node'),
    _ = require('lodash'),
    process = require('process'),
    path = require('path'),
    config = require('config'),
    fs = require('fs'),
    sleep = require('system-sleep'),
    KeyedMessage = kafka.KeyedMessage,
    translator = require('./transcribe-translator'),
    TranscribeClient = require('./transcribe-client');


console.log(`Config loaded : ${JSON.stringify(config)}`);

const CHUNK_STATUS = {
  ERROR: 'ERROR',
  IGNORED: 'IGNORED',
  SUCCESS: 'SUCCESS'
};

var completeJob = false;
var errorMes = '';
var media = '';
const transcribeClient = new TranscribeClient(config.speechmatics);

// Start Idle timer
console.log('Started with endIfIdleSecs config : '+ config.endIfIdleSecs);
let idleTimer = setTimeout(gracefulShutdown, config.endIfIdleSecs * 1000);

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
    if (consumerGroup)
        cleanUpResource.push(Promise.promisify(consumerGroup.close));
    if (producer)
        cleanUpResource.push(Promise.promisify(producer.close));
    if (client)
        cleanUpResource.push(Promise.promisify(client.close));

    Promise.all(cleanUpResource).then(res => {
        console.log('Close all resources');
        process.exit();
    }).catch(err => {
        console.log('Failed to close resource :', err);
        process.exit();
    });
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

    consumerGroup.on('message', message => {
        let t = process.hrtime();
        // Reset idle engine Timer
        clearTimeout(idleTimer);
        idleTimer = setTimeout(gracefulShutdown, config.endIfIdleSecs * 1000);

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

        // check language value
        if (typeof event.taskPayload === 'undefined' || typeof event.taskPayload.language === 'undefined') {
            event.taskPayload = {
                language: 'en-US'
            };
        }

        (async () => {
            try {
                await transcribeClient.createTranscribeJob(event.cacheURI, event.taskPayload.language, function createTranscribeJobCallback(err, response) {
                    if (err) {
                        return sendChunkProcessed(event, CHUNK_STATUS.ERROR, err, t);
                    }

                    // get jobId
                    var jobId = response.id;
                    console.log('Jobid got:', jobId);
                    var continueJob = true;
                    while (continueJob) {
                        sleep(20000);
                        transcribeClient.getTranscribeJobStatus(jobId, function getTranscribeJobStatusCallback(err, res) {
                            if (err) {
                                errorMes = err;
                                continueJob = false;
                            } else {
                                var jobStatus = res.job_status;
                                console.log('Status got :', jobStatus);
                                if (jobStatus === 'rejected' || jobStatus === 'unsupported_file_format' || jobStatus === 'could_not_align') {
                                    errorMes = 'file could not be processed';
                                    continueJob = false;
                                }
                                if (jobStatus === 'done') {
                                    // get Transcript
                                    transcribeClient.getTranscript(jobId, function getTranscriptCallback(err, transcript) {
                                        if (err) {
                                            errorMes = 'Could not get transcript';
                                        } else {
                                            //console.log('Transcript got :',transcript);
                                            media = transcript;
                                            completeJob = true;
                                        }
                                    });
                                    continueJob = false;
                                }
                            }
                        });
                    }

                    if (completeJob) {
                        handleResponse(media, function handleResponseCallback(err, taskOutput) {
                            if (err) {
                                return sendChunkProcessed(event, CHUNK_STATUS.ERROR, err, t);
                            }

                            const engineOutput = Object.assign({
                                type: 'engine_output',
                                timestampUTC: Date.now(),
                                outputType: 'object-series',
                                mimeType: 'application/json',
                                content: JSON.stringify({
                                    series: [taskOutput]
                                }),
                                rev: 1
                            }, _.pick(event, ['taskId', 'tdoId', 'jobId', 'startOffsetMs', 'endOffsetMs', 'taskPayload', 'chunkUUID']));
                            console.log(`${event.taskId} Sending the engine output... `);
                            console.log(`${event.taskId} Engine output: ${JSON.stringify(engineOutput)}`);


                            // Send successful chunk processed messages
                            return sendChunkProcessed(event, CHUNK_STATUS.SUCCESS, null, t);
                        });
                    } else {
                        return sendChunkProcessed(event, CHUNK_STATUS.ERROR, errorMes, t);
                    }
                });

            } catch (e) {
                console.log('Exception :',e);
                sendChunkProcessed(event, CHUNK_STATUS.ERROR, e, t);
                process.exit(1);
            }
        })();
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

    const chunkStatus = {
        type: 'chunk_processed_status',
        timestampUTC: Date.now(),
        taskId: event.taskId,
        chunkUUID: event.chunkUUID,
        status: status
    };

    if(mess) {
        console.log(mess);
        if (status === CHUNK_STATUS.SUCCESS || status === CHUNK_STATUS.IGNORED) {
            chunkStatus.infoMsg = _.isEmpty(mess) ? null : mess
        } else {
            chunkStatus.errorMsg = _.isEmpty(mess) ? null : mess
        }
    }

    console.log(`(TaskID: ${event.taskId}) Sending a chunk_processed_status to kafka: ${JSON.stringify(chunkStatus)}`);

    producer.send([{
        topic: config.kafka.topics.chunkQueue,
        messages: [
            new KeyedMessage(event.taskId, JSON.stringify(chunkStatus))
        ]
    }], (err, data) => {
        t = process.hrtime(t);
        if (err) {
            console.log(`(TaskID: ${event.taskId}) [chunk_processed_status] Failed to produce status to kafka: ${err}`);
            console.log(`(TaskID: ${event.taskId}) Total time elapsed: ${t[0]}s ${t[1]}ns`);

            return;
        }

        console.log(`(TaskID: ${event.taskId}) [chunk_processed_status] Produced status message: ${JSON.stringify(data)}`);
        console.log(`(TaskID: ${event.taskId}) Total time elapsed: ${t[0]}s ${t[1]}ns`);
    });
}

function handleResponse(res, callback) {
    console.log(`Saving transcript to asset`);
    const vlf = translator.latticeToVLF(res);
    vlf.forEach(function (v) {
       v.words.forEach(function (w) {
          w.bestPath = true;
          w.utteranceLength = 1;
       });
    });
    callback(null, vlf);
}




