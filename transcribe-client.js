var SpeechmaticsClient = require('./speechmatics-client');

function TranscribeClient(config) {
	if (!config || typeof config !== 'object') {
		throw new Error('Missing param: config');
	}
	if (!config.userId || typeof config.userId !== 'string') {
		throw new Error('Missing or invalid userId');
	}
	if (!config.apiKey || typeof config.apiKey !== 'string') {
		throw new Error('Missing or invalid apiKey');
	}
	if (!config.supportedInputs || typeof config.supportedInputs !== 'object') {
		throw new Error('Missing or invalid supportedInputs');
	}

	this._client = new SpeechmaticsClient(config.userId, config.apiKey, {});
	this._supportedInputs = config.supportedInputs;
}

TranscribeClient.prototype.createTranscribeJob = function createTranscribeJob(assetURI, language, callback) {
	console.log(`Start create job with assetURI :${assetURI} and language: ${language}`);

	var opts = {
		assetURI: assetURI,
		language: language
	};
	console.log(this._client);
	this._client.createJob(opts, function createJobCallback(err, resp) {
		if (err) {
			return callback(err);
		}
		callback(null, resp);
	});
};

TranscribeClient.prototype.getTranscribeJobStatus = function getTranscribeJobStatus(jobId, callback) {
	console.log(`Start get job status with jobid ${jobId}`);

	this._client.getJob(jobId, function getJobStatusCallback(err, resp) {
		if (err) {
			return callback(err);
		}
		if (!resp) {
			return callback(new Error(`Invalid getJob status response got`));
		}

		callback(null, resp);
	});
};

TranscribeClient.prototype.getTranscript = function getTranscript(jobId, callback) {
	console.log(`Start get transcript with jobid: ${jobId}`);
	this._client.getTranscript(jobId, function getTranscriptResult(err, resp) {
		if (err) {
			return callback(err);
		}
		if (!resp) {
			return callback(new Error(`Invalid response from getTranscript`));
		}
		if (!Array.isArray(resp.words)) {
			return callback(new Error(`Invalid transciption result`));
		}
		callback(null, resp.words);
	});
};

module.exports = TranscribeClient;
