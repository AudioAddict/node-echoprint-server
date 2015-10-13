require('newrelic');
var async = require('async');
var urlParser = require('url');
var log = require('winston');
var fingerprinter = require('./fingerprinter');
var server = require('../server');
var config = require('../config');

/**
 * Browser-friendly query debugging endpoint.
 */
exports.debugQuery = function(req, res) {
  if (!req.body || !req.body.json)
    return server.renderView(req, res, 200, 'debug.jade', {});

  var json, code, codeVer;
  try {
    json = JSON.parse(req.body.json);
    first = json[0];
    code = first.code;
    codeVer = first.metadata.version.toString();
  } catch (err) {
    log.warn('Failed to parse JSON debug input: ' + err);
  }

  if (!code || !codeVer || codeVer.length !== 4) {
    return server.renderView(req, res, 500, 'debug.jade',
      { err: 'Unrecognized input' });
  }

  if (req.body.Ingest) {
    req.body = json;
    return require('./api').ingestAll(req, res);
  } else {
    req.body = json;
    return require('./api').queryAll(req, res);
  }
};
