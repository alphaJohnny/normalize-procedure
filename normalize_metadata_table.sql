CREATE TABLE  `normalize_metadata` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `denorm_table` varchar(255) NOT NULL,
  `nf3_table` varchar(255) NOT NULL,
  `denorm_cols` varchar(255) NOT NULL COMMENT 'comma seperated list',
  `nf3_col_names` varchar(255) NOT NULL COMMENT 'comma seperated list',
  `denorm_keys` varchar(255) NOT NULL COMMENT 'comma seperated list',
  `nf3_biz_key` varchar(255) NOT NULL COMMENT 'the col to which the denorm key maps to in 3nf table',
  `nf3_identity_col` varchar(255) NOT NULL COMMENT 'denorm_keys + this col uniquely identifies the record',
  `nf3_identity_value` varchar(255) NOT NULL COMMENT 'use quotes to hard code the value else provide column name',
  `denorm_where` varchar(255) NOT NULL DEFAULT '1=1' COMMENT 'optional WHERE clause to assign to denorm table',
  `flags` bigint(20) unsigned NOT NULL,
  `is_completed` tinyint(1) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1
