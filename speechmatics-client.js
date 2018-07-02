'use strict'

var request = require('request');

function SpeechmaticsClient(userId , apiToken , opts) {
    if(!userId) throw new Error('UserId is required');
    if(!apiToken) throw new Error('Auth Token is required');

    this.userId = userId;
    this.apiToken = apiToken;

    opts = opts || {};
    this.baseUrl = opts.baseUrl || 'https://api.speechmatics.com';
    this.apiVersion = opts.apiVersion || '1.0';
    this.headers = opts.headers || {};
}

SpeechmaticsClient.prototype.makeRequest = function makeRequest(method , path, opts , callback) {
    var newOpts = mergeOptions({}, opts);

    path = path.replace(':userId', this.userId);
    console.log(`Fill path get : ${path}`);

    newOpts.method = method;
    newOpts.json = true;
    newOpts.headers = this.headers;
    newOpts.baseUrl = this.baseUrl;
    newOpts.url = '/v' + this.version + '/' + path;
    newOpts.qs = {};
    newOpts.qs.auth_token = this.apiToken;
    console.log(`New Options get : ${JSON.stringify(newOpts)}`);

    request(newOpts, function requestCallback(err, res, body) {
        if(err) {
            return callback(err);
        }
        if(res.statusCode >= 400) {
            return callback(res.body);
        }
        if(typeof body === 'object') {
            var keys = Object.keys(body);
            if (keys.length === 1) body = body[keys[0]];
        }
        callback(null, body);
    });
};

function mergeOptions(obj1, obj2) {
    var obj3 = {};
    for (var attr in obj1) {
        obj3[attr] = obj1[attr];
    }
    for (var attr1 in obj2) {
        obj3[attr1] = obj2[attr1];
    }
    return obj3;
}

SpeechmaticsClient.prototype.get = function get(path, opts, done) {
    this.makeRequest('GET', path, opts, done);
};

SpeechmaticsClient.prototype.post = function post(path, opts, done) {
    var fd = { model: opts.language || 'en-US', diarisation: 'false', notification: 'none'};
    var options = mergeOptions(fd, opts);

    if (options.assetURI) {
        options.data_file = request(opts.assetURI);
    }

    this.makeRequest('POST', path, { formData: options }, done);
};

// Create job
SpeechmaticsClient.prototype.createJob = function createJob(opts, done) {
    this.post('user/:userId/jobs/', opts, done);
}

// Get Job Detail
SpeechmaticsClient.prototype.getJob = function getJob(jobId, done) {
    this.get('user/:userId/jobs/' + jobId + '/', {}, done);
}

// Get Transcript
SpeechmaticsClient.prototype.getTranscript = function getTranscript(jobId, done) {
    this.get('user/:userId/jobs/' + jobId + '/transcript', {}, done);
}

module.exports = SpeechmaticsClient;
