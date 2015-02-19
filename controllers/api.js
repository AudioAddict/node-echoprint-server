require('newrelic');
var urlParser = require('url');
var log = require('winston');
var fingerprinter = require('./fingerprinter');
var server = require('../server');
var config = require('../config');

/**
 * Querying for the closest matching track.
 */
exports.query = function(req, res) {
  var url = urlParser.parse(req.url, true);
  var code = url.query.code;
  var codeVer = url.query.version;

  if (!code && req.body)
    code = req.body.code;
  else if (!code)
    return server.respond(req, res, 500, { error: 'Missing code' });

  if (!codeVer && req.body)
    codeVer = req.body.version || req.body.metadata.version;
  if (codeVer != config.codever)
    return server.respond(req, res, 500, { error: 'Missing or invalid version' });

  fingerprinter.decodeCodeString(code, function(err, fp) {
    if (err) {
      log.error('Failed to decode codes for query: ' + err);
      return server.respond(req, res, 500, { error: 'Failed to decode codes for query: ' + err });
    }

    fp.codever = codeVer;

    fingerprinter.bestMatchForQuery(fp, config.code_threshold, function(err, result) {
      if (err) {
        log.warn('Failed to complete query: ' + err);
        return server.respond(req, res, 500, { error: 'Failed to complete query: ' + err });
      }

      var duration = new Date() - req.start;
      log.debug('Completed lookup in ' + duration + 'ms. success=' +
        !!result.success + ', status=' + result.status);

      return server.respond(req, res, 200, { success: !!result.success,
        status: result.status, match: result.match || null, custom_id: result.custom_id });
    });
  });
};

/**
 * Adding a new track to the database.
 */
exports.ingest = function(req, res) {
  var code = req.body.code;
  var codeVer = req.body.version || req.body.metadata.version;
  var length = req.body.length || req.body.metadata.duration;
  var track = req.body.track;
  var artist = req.body.artist;
  var custom_id = req.body.custom_id;

  if (!code)
    return server.respond(req, res, 500, { error: 'Missing "code" field' });
  if (!codeVer)
    return server.respond(req, res, 500, { error: 'Missing "version" field' });
  if (codeVer != config.codever)
    return server.respond(req, res, 500, { error: 'Version "' + codeVer + '" does not match required version "' + config.codever + '"' });
  if (isNaN(parseInt(length, 10)))
    return server.respond(req, res, 500, { error: 'Missing or invalid "length" field' });

  fingerprinter.decodeCodeString(code, function(err, fp) {
    if (err || !fp.codes.length) {
      log.error('Failed to decode codes for ingest: ' + err);
      return server.respond(req, res, 500, { error: 'Failed to decode codes for ingest: ' + err });
    }

    fp.codever = codeVer;
    fp.track = track;
    fp.length = parseInt(length, 10);
    fp.artist = artist;
    fp.custom_id = custom_id;

    fingerprinter.ingest(fp, function(err, result) {
      if (err) {
        log.error('Failed to ingest track: ' + err);
        return server.respond(req, res, 500, { error: 'Failed to ingest track: ' + err });
      }

      var duration = new Date() - req.start;
      log.debug('Ingested new track in ' + duration + 'ms. track_id=' +
        result.track_id + ', artist_id=' + result.artist_id);

      result.success = true;
      return server.respond(req, res, 200, result);
    });
  });
};
