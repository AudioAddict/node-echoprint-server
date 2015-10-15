require('newrelic');
var urlParser = require('url');
var log = require('winston');
var fingerprinter = require('./fingerprinter');
var server = require('../server');
var config = require('../config');

exports.queryAll = function(req, res) {
  var codeList = req.body;
  var total = codeList.length;
  var results = [];
  var collected = 0;
  var collectResults = function(index, result) {
    results[index] = result;
    if (++collected == total) {
      logRequestTime(new Date() - req.start);
      return server.respond(req, res, 200, results);
    }
  };

  for (var i = 0; i < total; i++) {
    query(i, codeList[i].code, codeList[i].metadata.version, collectResults);
  }
};

/**
 * Querying for the closest matching track.
 */
exports.query = function(req, res) {
  var url = urlParser.parse(req.url, true);
  var code = url.query.code;
  var version = url.query.version;

  if (!code && req.body)
    code = req.body.code;

  if (!version && req.body)
    version = req.body.version || (req.body.metadata ? req.body.metadata.version : false)

  query(null, code, version, function(index, result) {
    logRequestTime(new Date() - req.start);
    return server.respond(req, res, (result.success ? 200 : 422), result);
  });
};

/**
 * INGEST ALL THE TRACKS!!!11
 */
exports.ingestAll = function(req, res) {
  var fpList = req.body;
  var total = fpList.length;

  // ensure every fingerprint has a track_id to map results back to
  for (var i = 0; i < total; i++) {
    if(fpList[i].track_id === undefined || fpList[i].track_id == "") {
      return server.respond(req, res, 422, "One or more fingerprints is missing a track_id");
    }
  }

  var results = {};
  var collected = 0;

  var collectResults = function(trackId, result) {
    results[trackId] = result;
    if (++collected == total) {
      logRequestTime(new Date() - req.start);
      return server.respond(req, res, 200, results);
    }
  };

  for (var i = 0; i < total; i++) {
    ingest(fpList[i].code,
      fpList[i].metadata.version,
      fpList[i].track_id,
      fpList[i].upc,
      fpList[i].isrc,
      fpList[i].metadata.filename,
      collectResults);
  }
};

/**
 * Adding a new track to the database.
 */
exports.ingest = function(req, res) {
  var code = req.body.code;
  var version = req.body.version || req.body.metadata.version;
  var upc = req.body.upc;
  var isrc = req.body.isrc;
  var trackId = req.body.track_id || req.body.custom_id;

  ingest(code, version, trackId, upc, isrc, req.body.metadata.filename, function(trackId, result) {
    logRequestTime(new Date() - req.start);
    return server.respond(req, res, (result.success ? 200 : 422), result);
  });
};

function ingest(code, version, trackId, upc, isrc, filename, cb) {
  if (!code)
    return cb(trackId, error('Missing "code" field'));
  if (version != config.codever)
    return cb(trackId, error('Version "' + version + '" does not match required version "' + config.codever + '"'));
  if (!trackId || trackId == "")
    return cb(trackId, error('Missing "trackId" field'));

  return fingerprinter.decodeCodeString(code, function(err, fp) {
    if (err || !fp.codes.length) {
      return cb(trackId, error('Failed to decode codes for ingest: ' + err));
    }

    fp.metadata = {
      filename: filename,
      trackId: trackId,
      upc: upc,
      isrc: isrc
    };
    fp.version = version;

    return fingerprinter.ingest(fp, function(err, result) {
      if (err) {
        return cb(trackId, error('Failed to ingest track: ' + err));
      }

      return cb(trackId, result);
    });
  });
}

function query(index, code, version, cb) {
  if (!code)
    return cb(index, error('Missing code'));
  if (version != config.codever)
    return cb(index, error('Missing version'));

  fingerprinter.decodeCodeString(code, function(err, fp) {
    if (err) {
      return cb(index, error('Failed to decode codes for query: ' + err));
    }

    fp.version = version;
    fingerprinter.findMatches(fp, function(err, status, bestMatch, matches) {
      if (err) {
        return cb(index, error('Failed to complete query: ' + err));
      }

      var queryResult = {
        best_match: bestMatch && newMatchResult(bestMatch),
        debug_status: status,
        matches: []
      };

      if (matches && matches.length > 0) {
        for (var i = 0, total = matches.length; i < total; i++) {
          queryResult.matches.push(newMatchResult(matches[i]));
        }
      }

      return cb(index, queryResult);
    });
  });
}

function newMatchResult(match) {
  return {
    id: match.id,
    track_id: match.trackId,
    confidence: match.confidence,
    filename: match.filename,
    ingestedAt: match.ingestedAt
  };
}

function logRequestTime(reqDuration) {
  log.debug('Request finished in ' + reqDuration + 'ms');
}

function error(msg) {
  log.error(msg);
  return { error: msg };
}
