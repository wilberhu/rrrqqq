CREATE TABLE `tush_hist_data` (
  `ts_code` varchar(255) NOT NULL,
  `trade_date` datetime(6) NOT NULL,
  `open` float(10) NULL,
  `high` float(10) NULL,
  `low` float(10) NULL,
  `close` float(10) NULL,
  `pre_close` float(10) NULL,
  `change` float(10) NULL,
  `pct_chg` float(10) NULL,
  `vol` int NULL,
  `amount` float(20) NULL,
  PRIMARY KEY (`ts_code`, `trade_date`)
);