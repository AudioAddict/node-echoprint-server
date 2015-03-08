require('newrelic');
var fs = require('fs');
var solr = require('solr-client');
var temp = require('temp');
var log = require('winston');
var config = require('../config');
var log = require('winston');
var _ = require('underscore');

exports.fpQuery = fpQuery;
exports.getTrack = getTrack;
exports.getTrackByName = getTrackByName;
exports.getArtist = getArtist;
exports.getArtistByName = getArtistByName;
exports.addTrack = addTrack;
exports.addArtist = addArtist;
exports.updateTrack = updateTrack;
exports.updateArtist = updateArtist;
exports.disconnect = disconnect;

var solrClient = solr.createClient(config.solr_hostname, config.solr_port, config.solr_corename);
solrClient.basicAuth(config.solr_username, config.solr_password);

function fpQuery(fp, rows, callback) {
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
}

function getTrack(trackID, callback) {
  solrClient.realTimeGet(trackID, function(err, obj){
    if (err) return callback(err, null);
    if (obj.response.numFound === 1)
      return callback(null, obj.response.docs);
    else
      return callback(null, null);
  });
}

function getArtist(artistID, callback) {
  log.debug('Direct artist access is not implemented. getArtist is a noop.');
  return callback(null, null);
}

function addTrack(artist, fp, callback) {
  var length = fp.length;
  if (typeof length === 'string')
    length = parseInt(length, 10);

  track = {
    track: fp.track,
    track_id: fp.custom_id,
    artist: artist, 
    import_date: new Date(),
    length: length,
    source: 'echoprint-codegen',
    codes: fp.codes,
    times: fp.times,
    codever: fp.codever
  }

  solrClient.add(track, function(err, obj){
    if(err){
      return callback(err, null);
    } else {
      return callback(null, track.track_id);
    }
  });
}

function addArtist(artist, callback) {
  log.debug('Direct artist access is not implemented. getArtist is a noop.');
  return callback(null, null);
}

function getArtistByName(artist, callback) {
  log.debug('Direct artist access is not implemented. getArtist is a noop.');
  return callback(null, null);
}

function getTrackByName(track, artistID, callback) {
  var query = solrClient.createQuery().q({track: track}).fl('*,score').start(0).rows(1);

  solrClient.search(query, function(err, results){
    if (err) return callback(err, null);
    if (!results || !(results.response.numFound >= 1)) return callback(null, []);

    matches = results.response.docs
    callback(null, matches);
  });
}

function updateTrack(trackID, name, artistID, callback) {
  log.debug('Direct artist access is not implemented. getArtist is a noop.');
  return callback(null, null);
}

function updateArtist(artist, callback) {
  log.debug('Direct artist access is not implemented. getArtist is a noop.');
  return callback(null, null);
}

function disconnect(callback) {
  return callback();
}

