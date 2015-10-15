/**
 * MySQL database backend. An alternative database backend can be created
 * by implementing all of the methods exported by this module
 */

var mysql = require('mysql');
var _ = require('underscore');
var config = require('../config');

exports.fpQuery = fpQuery;
exports.addFingerprint = addFingerprint;

var client = mysql.createPool({
  connectionLimit: 50,
  user: config.db_user,
  password: config.db_pass,
  database: config.db_database,
  host: config.db_host,
  multipleStatements: true
});

/**
 *
 */
function fpQuery(fp, minScore, limit, callback) {
  var fpCodesStr = _.uniq(fp.codes).join(',');

  var sql = 'select track_id, upc, isrc, filename, count(distinct track_id, code) score ' +
    'from codes left join tracks on tracks.id=codes.track_id where code in (' + fpCodesStr + ') ' +
    'group by track_id having score >= ' + minScore + ' order by score desc ' +
    'limit ' + limit + ';'

  client.query(sql, function(err, matches) {
    if (err) return callback(err, null);
    if (!matches || !matches.length) return callback(null, []);

    var trackIdStr = _.map(matches, function(row) { return row.track_id }).join(',')
    sql = 'select track_id, group_concat(code) codes, group_concat(time) times ' +
      'from codes where track_id in (' + trackIdStr + ') group by track_id;'
    client.query(sql, function(err, codeMatches) {
      if (err) return callback(err, null);
      if (!codeMatches || !codeMatches.length) return callback(null, []);

      for (var i = 0; i < codeMatches.length; i++) {
        var match = _.findWhere(matches, {track_id: codeMatches[i].track_id});
        match.codes = codeMatches[i].codes.split(',');
        match.times = codeMatches[i].times.split(',');
      }

      callback(null, matches);
     });
  });
}

function addFingerprint(fp, callback) {
  var trackId = client.escape(fp.metadata.trackId);
  var sql = 'SET autocommit=0;';
  sql += 'INSERT INTO tracks (id,version,upc,isrc,filename) VALUES (' +
    trackId + ',' +
    client.escape(fp.version) + ',' +
    client.escape(fp.metadata.upc) + ',' +
    client.escape(fp.metadata.isrc) + ',' +
    client.escape(fp.metadata.filename) + ');';

  sql += 'INSERT INTO codes VALUES ';
  var values = [];
  for (var i = 0; i < fp.codes.length; i++) {
    values.push( '(' + fp.codes[i] + ',' + fp.times[i] + ',' + trackId + ')' );
  }
  sql += values.join(',') + ';commit;'

  client.query(sql, function(err, resp) {
    if (err) return callback(err, null);

    var result = {
      success: (resp[1].affectedRows === 1),
      trackId: fp.metadata.trackId
    };

    callback(null, result);
  });
}
