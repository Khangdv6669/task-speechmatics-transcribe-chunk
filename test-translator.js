var translator = require('./transcribe-translator');

var words = [{
    duration: '1.000',
    confidence: '0.943',
    name: 'Hello',
    time: '1.000'
}, {
    duration: '1.000',
    confidence: '0.995',
    name: 'world',
    time: '1.000'
}, {
    duration: '0.000',
    confidence: null,
    name: '.',
    time: '2.000'
}];

var vlf = translator.latticeToVLF(words);
console.log(vlf.toTranscript().toXml());
