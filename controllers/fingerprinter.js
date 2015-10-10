require('newrelic');
var zlib = require('zlib');
var log = require('winston');
var config = require('../config');
var database = require('../models/solr');

// Constants
var MAX_INGEST_DURATION = 60 * 60 * 4;
var SECONDS_TO_TIMESTAMP = 43.45;
var MAX_ROWS = 30;
var MATCH_SLOP = 2;

// min threshold for raw db matches
var MIN_MATCH_SCORE_PERCENT = 0.20;

// min theshold % for "actual score" histogram matches
var MIN_MATCH_CONFIDENCE = 0.30 * 100;

// Exports
exports.decodeCodeString = decodeCodeString;
exports.cutFPLength = cutFPLength;
exports.getCodesToTimes = getCodesToTimes;
exports.findMatches = findMatches;
exports.ingest = ingest;
exports.SECONDS_TO_TIMESTAMP = SECONDS_TO_TIMESTAMP;
exports.MATCH_SLOP = MATCH_SLOP;

/**
 * Takes a base64 encoded representation of a zlib-compressed code string
 * and passes a fingerprint object to the callback.
 */
function decodeCodeString(codeStr, callback) {
  // Fix url-safe characters
  codeStr = codeStr.replace(/-/g, '+').replace(/_/g, '/');

  // Expand the base64 data into a binary buffer
  var compressed = new Buffer(codeStr, 'base64');

  // Decompress the binary buffer into ascii hex codes
  zlib.inflate(compressed, function(err, uncompressed) {
    if (err) return callback(err, null);
    // Convert the ascii hex codes into codes and time offsets
    var fp = inflateCodeString(uncompressed);
    fp.codeStr = codeStr;
    log.debug('Inflated ' + codeStr.length + ' byte code string into ' +
      fp.codes.length + ' codes');

    callback(null, fp);
  });
}

/**
 * Takes an uncompressed code string consisting of zero-padded fixed-width
 * sorted hex integers and converts it to the standard code string.
 */
function inflateCodeString(buf) {
  // 5 hex bytes for hash, 5 hex bytes for time (40 bits per tuple)
  var count = Math.floor(buf.length / 5);
  var endTimestamps = count / 2;
  var i;

  var codes = new Array(count / 2);
  var times = new Array(count / 2);

  for (i = 0; i < endTimestamps; i++) {
    times[i] = parseInt(buf.toString('ascii', i * 5, i * 5 + 5), 16);
  }
  for (i = endTimestamps; i < count; i++) {
    codes[i - endTimestamps] = parseInt(buf.toString('ascii', i * 5, i * 5 + 5), 16);
  }

  // Sanity check
  for (i = 0; i < codes.length; i++) {
    if (isNaN(codes[i]) || isNaN(times[i])) {
      log.error('Failed to parse code/time index ' + i);
      return { codes: [], times: [] };
    }
  }

  return { codes: codes, times: times };
}

/**
 * Clamp this fingerprint to a maximum N seconds worth of codes.
 */
function cutFPLength(fp, maxSeconds) {
  if (!maxSeconds) maxSeconds = 180;

  var newFP = {};
  for(var key in fp) {
    if (fp.hasOwnProperty(key))
     newFP[key] = fp[key];
   }

  var firstTimestamp = fp.times[0];
  var sixtySeconds = maxSeconds * SECONDS_TO_TIMESTAMP + firstTimestamp;

  for (var i = 0; i < fp.times.length; i++) {
    if (fp.times[i] > sixtySeconds) {
      log.debug('Clamping ' + fp.codes.length + ' codes to ' + i + ' codes');

      newFP.codes = fp.codes.slice(0, i);
      newFP.times = fp.times.slice(0, i);
      return newFP;
    }
  }

  newFP.codes = fp.codes.slice(0);
  newFP.times = fp.times.slice(0);
  return newFP;
}

/**
 * Finds the closest matching tracks, if any, to a given fingerprint.
 */
function findMatches(fp, threshold, callback) {
  fp = cutFPLength(fp);

  if (!fp.codes.length)
    return callback('No valid fingerprint codes specified', null);

  log.debug('Starting query with ' + fp.codes.length + ' codes');

  database.fpQuery(fp, MAX_ROWS, function(err, matches) {
    if (err) return callback(err);

    if (!matches || !matches.length) {
      log.debug('No matched fingerprints from DB');
      return callback(null, 'NO_DB_RESULTS');
    }

    log.debug('Matched ' + matches.length + ' tracks, top code overlap is ' + matches[0].score);
    log.debug('Need score > ' + fp.codes.length * MIN_MATCH_SCORE_PERCENT);

    // If the best result matched fewer codes than our percentage threshold,
    // report no results
    if (matches[0].score < fp.codes.length * MIN_MATCH_SCORE_PERCENT) {
      log.debug('No matched tracks above minimum score threshold');
      return callback(null, 'NO_MIN_RESULTS');
    }

    // Compute more accurate scores for each track by taking time offsets into account
    var newMatches = [];
    for (var i = 0; i < matches.length; i++) {
      var match = matches[i];

      // get a confidence rating of 0-100 by examining the time code histogram
      match.confidence = getActualScore(fp, match, threshold, MATCH_SLOP);

      // filter results that don't meet the minimum histogram threshold
      if (match.confidence >= MIN_MATCH_CONFIDENCE)
        newMatches.push(match);
    }
    matches = newMatches;

    if (!matches.length) {
      log.debug('No matched tracks after confidence score adjustment');
      return callback(null, 'NO_CONFIDENT_RESULTS');
    }

    // Sort the matches based on confidence
    matches.sort(function(a, b) { return b.confidence - a.confidence; });

    var exactMatch = null;
    if (matches[0].confidence >= 100) {
      if (matches.length == 1 || (matches[0].confidence > matches[1].confidence)) {
        exactMatch = matches.shift();
        exactMatch.confidence = clampConfidence(exactMatch.confidence);
      }
    }

    for (var i = 0; i < matches.length; i++) {
      matches[i].confidence = clampConfidence(matches[i].confidence);
    }

    var status = "MULTIPLE_GOOD_RESULTS";
    if (exactMatch) {
      status = "EXACT_MATCH";
      if (matches.length) {
        status += "_MULTIPLE_RESULTS";
      }
    }

    callback(null, status, exactMatch, matches);
  });
}

// clamp the confidence score between 0-100%, we do this
// after determining a winner to account for scores resulting in > 100% code matches
function clampConfidence(value) {
  return Math.min(Math.max(value, 0), 100)
}

/**
 * Build a mapping from each code in the given fingerprint to an array of time
 * offsets where that code appears, with the slop factor accounted for in the
 * time offsets. Used to speed up getActualScore() calculation.
 */
function getCodesToTimes(match, slop) {
  var codesToTimes = {};

  var code, time;
  for (var i = 0; i < match.codes.length; i++) {
    code = match.codes[i];
    time = Math.floor(match.times[i] / slop) * slop;

    if (codesToTimes[code] === undefined)
      codesToTimes[code] = [];
    codesToTimes[code].push(time);
  }

  return codesToTimes;
}

/**
 * Computes the actual match score for a track by taking time offsets into
 * account.
 */
function getActualScore(fp, match, threshold, slop) {
  if (match.codes.length < threshold)
    return 0;

  var timeDiffs = {};
  var code, time, matchTimes, dist, i, j;

  var numCodes = fp.codes.length;
  var matchCodesToTimes = getCodesToTimes(match, slop);

  // Iterate over each {code,time} tuple in the query
  for (i = 0; i < numCodes; i++) {
    code = fp.codes[i];
    time = Math.floor(fp.times[i] / slop) * slop;

    // get all of time offsets where this code appeared
    matchTimes = matchCodesToTimes[code];
    if (matchTimes) {
      for (j = 0; j < matchTimes.length; j++) {
        dist = Math.abs(time - matchTimes[j]);
        // Increment the histogram bucket for this distance
        if (timeDiffs[dist] === undefined)
          timeDiffs[dist] = 0;
        timeDiffs[dist]++;
      }
    }
  }

  //match.histogram = timeDiffs;

  // Convert the histogram into an array, sort it, and sum the top two
  // frequencies to compute the adjusted score
  var keys = Object.keys(timeDiffs);
  var array = new Array(keys.length);
  for (i = 0; i < keys.length; i++)
    array[i] = [ keys[i], timeDiffs[keys[i]] ];
  array.sort(function(a, b) { return b[1] - a[1]; });

  var score = 0;
  if (array.length > 1)
    score = array[0][1] + array[1][1];
  else if (array.length === 1)
    score = array[0][1];

  // convert score to percentage, allow > 100 for cases where the two top histogram
  // scores combined result in 100%+ in order to compare better against potential results
  // that are 100% (identical vas nearly identical)
  return +(score / numCodes * 100).toFixed(2);
}

/**
 * Takes a track fingerprint (includes codes and time offsets plus any
 * available metadata), adds it to the database and returns a track_id,
 * artist_id, and artist name if available.
 */
function ingest(fp, callback) {
  log.info('Ingesting track_id=' + fp.track_id + ' UPC=' + fp.upc +
    ' ISRC=' + fp.isrc + ' (' + fp.codes.length + ' codes)');

  if (!fp.codes.length)
    return callback('Missing "codes" array', null);
  if (!fp.version)
    return callback('Missing or invalid "version" field', null);
  if (!fp.metadata.trackId)
    return callback('Missing or invalid "trackId" field', null);

  fp = cutFPLength(fp, MAX_INGEST_DURATION);
  database.addFingerprint(fp, function(err, result) {
    if (err) { return callback(err, null); }
    callback(null, result);
  });
}
