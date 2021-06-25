CREATE TABLE tush_trade_cal (
    exchange VARCHAR ( 255 ),
	cal_date date NOT NULL,
	is_open int,
	pretrade_date date,
	PRIMARY KEY (`cal_date`)
)
ENGINE = INNODB CHARACTER
SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;