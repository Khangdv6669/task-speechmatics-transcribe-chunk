'use strict'
/*
 * convert transcript to vlf files
 */
var VLF = require('vlf');

function convertTranscriptionResponseToVLF(transcript) {
    var vlf = new VLF();
    console.log(`Transcript for translating to vlf ${transcript}`);
    if(!transcript) {
        throw new Error(`Missing argument transcript`);
    }

    transcript.forEach(function eachTranscriptionWord(word) {
        var vlfWord = {
            word: word.name,
            confidence: parseFloat(word.confidence),
            bestPathForward: true,
            bestPathBackward: true,
            spanningForward: false,
            spanningBackward: false,
            spanningLength: 1
        };
        var timeStart = Math.floor(word.time * 1000);
        var timeEnd = Math.floor((word.time * 1000) + (word.duration * 1000));

        vlf.appendPath(timeStart, timeEnd, [vlfWord]);
    });

    return vlf;
}

module.exports = {
  latticeToVLF: convertTranscriptionResponseToVLF
};
