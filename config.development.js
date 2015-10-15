/**
 * Development configuration variables
 */

module.exports = {
 // Port that the web server will bind to
  web_port: 37760,

  // Database settings
  solr_hostname: 'vagrant-env-platform',
  solr_port: 8980,
  solr_max_boolean_terms: 1024,

    // Database settings
  db_user: 'echoprint',
  db_pass: 'Ph_F?du7ucAPev',
  db_database: 'echoprint',
  db_host: 'colossus.audioaddict.com',

  // Set this to a system username to drop root privileges
  run_as_user: 'vagrant',

  // Filename to log to
  log_path: __dirname + '/logs/echoprint.log',
  // Log level. Valid values are debug, info, warn, error
  log_level: 'debug',

  // Supported version of echoprint-codegen codes
  codever: '4.12'
};
