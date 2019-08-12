'use strict'

const _ = require('lodash'),
	path = require('path'),
	config = require('config'),
	sleep = require('system-sleep'),
	Promise = require('bluebird'),
	express = require('express'),
	multer = require('multer'),
	translator = require('./transcribe-translator'),
	TranscribeClient = require('./transcribe-client');

console.log(`Config loaded : ${JSON.stringify(config)}`);

var completeJob = false;
var errorMes = '';
var media = '';
const transcribeClient = new TranscribeClient(config.speechmatics);

server();

function server() {
	const app = express();
	const upload = multer({storage: multer.memoryStorage()});
	app.listen(8080);

	app.get('/readyz', (req, res) => {
		res.status(200).send('OK');
	});

	app.post('/process', upload.single('chunk'), async (req, res) => {
		console.log('Engine start processing');
		let chunk_data = req.body;
		try {
			let series = [];
			await run(chunk_data).then((series) => {
				console.log(`Final series: ${JSON.stringify(series)}`);
				res.status(200);
				return res.send({series: series});
			},(err) => {
				console.log(JSON.stringify(err));
				res.status(500);
				return res.send(err.data);
			})
		} catch (e) {
			console.log(JSON.stringify(e.data));
			res.status(500);
			return res.send(e.data);
		}
	});

	return app;
}

function run(message) {
	return new Promise((resolve, reject) => {
		console.log('Message got :', message);
		// validate kafka message
		const error = validateEvent(message);
		if (!_.isEmpty(error)) {
			return reject(error);
		}

		// check language value
		if (typeof message.taskPayload === 'undefined' || typeof message.taskPayload.language === 'undefined') {
			message.taskPayload = {
				language: 'en-US'
			};
		}
		(async () => {
			try {
				const createTranscribeJobPromised = Promise.promisify(
					transcribeClient.createTranscribeJob,
					{context: transcribeClient}
				);
				let response = await createTranscribeJobPromised(message.cacheURI, message.taskPayload.language).catch(err => {
					return reject(err);
				});

				var jobId = response.id;
				console.log('Jobid got:', jobId);
				var continueJob = true;
				var count = 0;

				while (continueJob && (count < 10)) {
					sleep(10000);
					const getTranscribeJobStatusPromised = Promise.promisify(
						transcribeClient.getTranscribeJobStatus,
						{context: transcribeClient}
					);
					let res = await getTranscribeJobStatusPromised(jobId).catch(err => {
						errorMes = JSON.stringify(err);
						continueJob = false;
					});

					var jobStatus = res.job_status;
					console.log('Status got :', jobStatus);
					if (jobStatus === 'rejected' || jobStatus === 'unsupported_file_format' || jobStatus === 'could_not_align') {
						errorMes = 'file could not be processed';
						continueJob = false;
					}
					if (jobStatus === 'done') {
						const getTranscriptPromised = Promise.promisify(
							transcribeClient.getTranscript,
							{context: transcribeClient}
						);
						let transcript = await getTranscriptPromised(jobId).catch(err => {
							errorMes = JSON.stringify(err);
							continueJob = false;
						});

						media = transcript;
						completeJob = true;
						continueJob = false;
					}
					count++;
				}

				if (completeJob) {
					handleResponse(media, function handleResponseCallback(err, taskOutput) {
						if (err) {
							return reject(err);
						}
						resolve(taskOutput);
					});
				} else {
					return reject(new Error(errorMes));
				}
			} catch (e) {
				return reject(e);
			}
		})();
	});
};

/**
 * Validata if kafka message is of right type and mimeType
 */

function validateEvent(event) {
	let errs = [];
	console.log(event.cacheURI);
	let fileType = path.extname(event.cacheURI).substring(1).toLowerCase();
	console.log(`File type :${fileType} and supported  :${config.speechmatics.supportedInputs[fileType]}`);
	if (!config.speechmatics.supportedInputs[fileType]) {
		errs.push(`File type was not supprot`);
	}
	return errs;
}

function handleResponse(res, callback) {
	console.log(`Saving transcript to asset`);
	const vlf = translator.latticeToVLF(res);
	const result = [];
	vlf.forEach(function (v) {
		v.words.forEach(function (w) {
			w.bestPath = true;
			w.utteranceLength = 1;
		});
		result.push(v);
	});
	callback(null, result);
}
