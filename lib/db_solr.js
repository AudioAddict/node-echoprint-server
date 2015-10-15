require('newrelic');
var solr = require('solr-client');
var _ = require('underscore');

var config = require('../config');

var solrClient = solr.createClient(config.solr_hostname, config.solr_port, config.solr_corename);
solrClient.basicAuth(config.solr_username, config.solr_password);

exports.fpQuery = function(fp, noop, rows, callback) {
  //gMutex.lock(function() {

    var fpCodesStr = _.uniq(fp.codes).slice(0, (config.solr_max_boolean_terms - 1)).join(' ');
    //var fpCodesStr = fp.codes.slice(0, (config.solr_max_boolean_terms - 1)).join(' ');

    // Get the top N matching tracks sorted by score (number of matched codes)
    var query = solrClient.createQuery().q({codes: fpCodesStr }).fl('*').start(0).rows(rows);

    solrClient.search(query, function(err, results){
      if (err) {
        gMutex.release();
        return callback(err, null);
      }
      if (!results || !results.response.numFound >= 1) return callback(null, []);

      var codeMatches = results.response.docs;
      var matches = []
      var numDocs = codeMatches.length;

      for (var i = 0; i < numDocs; i++) {
        matches[i] = codeMatches[i];
        matches[i].score = _.intersection(codeMatches[i].codes, fp.codes).length;
      }

      //gMutex.release();
      callback(null, matches);
    });

  //});
};

exports.addFingerprint = function(fp, callback) {
  var document = {
    upc: fp.metadata.upc,
    isrc: fp.metadata.isrc,
    trackId: fp.metadata.trackId,
    filename: fp.metadata.filename,
    version: fp.version,
    codes: fp.codes,
    times: fp.times
  };

  solrClient.add(document, function(err, obj) {
    var result = {
      success: !!(obj.responseHeader.status == 0),
      trackId: document.trackId
    };

    callback(err, result);
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
  var query = solrClient.createQuery().q(query).fl('*').start(0).rows(limit);

  solrClient.search(query, function(err, results){
    if (err) return callback(err, null);
    if (!results || !(results.response.numFound >= 1)) return callback(null, []);

    matches = results.response.docs
    callback(null, matches);
  });
};
