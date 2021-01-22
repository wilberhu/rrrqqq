CREATE TABLE tush_trade_cal (
    exchange VARCHAR ( 255 ),
	cal_date VARCHAR ( 255 ) NOT NULL,
	is_open VARCHAR ( 255 ),
	pretrade_date VARCHAR ( 255 ),
	PRIMARY KEY (`cal_date`)
)
ENGINE = INNODB CHARACTER
SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;