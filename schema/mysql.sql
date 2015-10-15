CREATE TABLE IF NOT EXISTS `tracks` (
  `id` mediumint unsigned NOT NULL,
  `version` char(4) NOT NULL,
  `upc` varchar(25) DEFAULT NULL,
  `isrc` varchar(25) DEFAULT NULL,
  `filename` varchar(1024) DEFAULT NULL,
  `ingest_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  index (upc),
  index (isrc)
) DEFAULT CHARSET=utf8, engine=myisam;

CREATE TABLE IF NOT EXISTS `codes` (
  `code` mediumint unsigned NOT NULL,
  `time` mediumint unsigned NOT NULL,
  `track_id` mediumint unsigned NOT NULL,
  KEY track_code (`code`,`track_id`)
) DEFAULT CHARSET=utf8, engine=myisam;

# required changes in my.cnf
# group_concat_max_len=1048576
