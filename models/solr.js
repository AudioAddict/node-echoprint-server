require('newrelic');
var fs = require('fs');
var solr = require('solr-client');
var temp = require('temp');
var log = require('winston');
var config = require('../config');
var log = require('winston');
var _ = require('underscore');

var solrClient = solr.createClient(config.solr_hostname, config.solr_port, config.solr_corename);
solrClient.basicAuth(config.solr_username, config.solr_password);

exports.fpQuery = function(fp, rows, callback) {
  var fpCodesStr = fp.codes.slice(0, (config.solr_max_boolean_terms - 1)).join(' ');

  // Get the top N matching tracks sorted by score (number of matched codes)
  var query = solrClient.createQuery().q({codes: fpCodesStr }).fl('*,score').start(0).rows(rows);

  solrClient.search(query, function(err, results){
    if (err) return callback(err, null);
    if (!results || !results.response.numFound >= 1) return callback(null, []);

    var codeMatches = results.response.docs;
    var matches = []

    for (var i = 0; i < codeMatches.length; i++) {
      matches[i] = codeMatches[i];
      matches[i].score = _.intersection(codeMatches[i].codes, fp.codes).length;
    }

    callback(null, matches);
  });
};

exports.addFingerprint = function(fp, callback) {
  var document = {
    upc: fp.metadata.upc,
    isrc: fp.metadata.isrc,
    trackId: fp.metadata.trackId,
    filename: fp.metadata.filename,
    codes: fp.codes,
    times: fp.times,
    version: fp.version
  }

  solrClient.add(document, function(err, obj){
    var result = {
      success: !!(obj.responseHeader.status == 0),
      trackId: document.trackId
    };

    return callback(err, result);
  });
};

exports.getFpById = function(trackId, callback) {
  solrClient.realTimeGet(trackId, function(err, obj){
    if (err) return callback(err, null);
    if (obj.response.numFound === 1)
      return callback(null, obj.response.docs);
    else
      return callback(null, null);
  });
};

exports.find = function(query, callback, limit) {
  limit = limit || 1;
  var query = solrClient.createQuery().q(query).fl('*,score').start(0).rows(limit);

  solrClient.search(query, function(err, results){
    if (err) return callback(err, null);
    if (!results || !(results.response.numFound >= 1)) return callback(null, []);

    matches = results.response.docs
    callback(null, matches);
  });
};
