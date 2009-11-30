DROP TABLE IF EXISTS `rec_array`;
CREATE TABLE `rec_array` (
  `id` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`)
);

DROP TABLE IF EXISTS `rec_array_item`;
CREATE TABLE `rec_array_item` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `array` bigint(20) NOT NULL,
  `pos` bigint(20) NOT NULL,
  `value_data` varchar(255) DEFAULT NULL,
  `value_type` enum('array','data','hash','text','value') NOT NULL DEFAULT 'value',
  PRIMARY KEY (`id`),
  UNIQUE KEY `array_2` (`array`,`pos`)
);

DROP TABLE IF EXISTS `rec_hash`;
CREATE TABLE `rec_hash` (
  `id` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`)
);

DROP TABLE IF EXISTS `rec_hash_item`;
CREATE TABLE `rec_hash_item` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `hash` bigint(20) NOT NULL,
  `key_data` varchar(255) DEFAULT NULL,
  `key_hash` varchar(22) NOT NULL,
  `key_type` enum('text','value') NOT NULL DEFAULT 'value',
  `value_data` varchar(255) DEFAULT NULL,
  `value_type` enum('array','data','hash','text','value') NOT NULL DEFAULT 'value',
  PRIMARY KEY (`id`),
  UNIQUE KEY `hash_2` (`hash`,`key_hash`)
);

DROP TABLE IF EXISTS `rec_item`;
CREATE TABLE `rec_item` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `item_type` enum('array','hash') NOT NULL DEFAULT 'hash',
  PRIMARY KEY (`id`)
);

DROP TABLE IF EXISTS `rec_value_data`;
CREATE TABLE `rec_value_data` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `data` longblob NOT NULL,
  PRIMARY KEY (`id`)
);

DROP TABLE IF EXISTS `rec_value_text`;
CREATE TABLE `rec_value_text` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `data` longtext NOT NULL,
  PRIMARY KEY (`id`)
);

