/* query prod */

CREATE EXTERNAL TABLE IF NOT EXISTS investio_prod.earnings_calendar (
  
  /*,date,date formatted,Comp,company_sticker,EPS,EPS formatted,EPS frc,EPS frc formatted,EPS surprise,Revenues,Revenues formatted,Revenues frc,Revenues frc formatted,Revenues surprise,Mkt Cap,Mkt Cap formatted*/
  `iteration` int,
  `date` string,
  `date_formatted` string,
  `Comp` string,
  `ticker` string,
  `EPS` string,
  `EPS_formatted` string,
  `EPS_frc` string,
  `EPS_frc formatted` string,
  `EPS_surprise` string,
  `Revenues` string,
  `Revenues_formatted` string,
  `Revenues_frc formatted` string,
  `Revenues_surprise` string,
  `Mkt_Cap` string,
  `Mkt_Cap_formatted` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ',',
  'quoteChar' = '"'
) LOCATION 's3://invest-io/sr-investio-all-dwh/earnings-calendar/'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS investio_prod.dividends_calendar (
  
  `interation` int,
  `Company` string,
  `Ticker` string,
  `Ex-dividend date` string,
  `Dividend` string,
  `Type` string,
  `Payment date` string,
  `Yield` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ',',
  'quoteChar' = '"'
) LOCATION 's3://invest-io/sr-investio-all-dwh/dividends-calendar'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS investio_prod.candles (
  /*Date,High,Low,Open,Close,Volume,Adj Close,Ticker*/
  `date` string,
  `High` float,
  `Low` float,
  `Open` float,
  `Close` float,
  `Volume` float,
  `Adj close` float,
  `Ticker` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ',',
  'quoteChar' = '"'
) LOCATION 's3://invest-io/sr-investio-all-dwh/candles'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS investio_prod.tickers (
  /*Symbol,Company_Name1,Currency,Capitalization,1st Jan %,Sector*/
  `Ticker` string,
  `Company` string,
  `Currency` string,
  `Capitalization` int,
  `1st Jan %` string,
  `Sector` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ',',
  'quoteChar' = '"'
) LOCATION 's3://invest-io/sr-investio-all-dwh/tickers'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS investio_prod.economic_calendar (
  
  /*	date	new_date	currency formatted	importance	importance formatted	event	actual	forecast	previous	actual formatted	forecast formatted	previous formatted	surprise
*/
  `iteration` int,
  `date` string,
  `new_date` string,
  `currency` string,
  `importance` string,
  `importance formatted` string,
  `event` string,
  `actual` string,
  `forecast` string,
  `previous` string,
  `Actual formatted` float,
  `Forecast formatted` float,
  `Previous formatted` float,
  `surprise` string
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ',',
  'quoteChar' = '"'
) LOCATION 's3://invest-io/sr-investio-all-dwh/economic-calendar/'
TBLPROPERTIES ('skip.header.line.count'='1');


CREATE EXTERNAL TABLE IF NOT EXISTS investio_prod.investing_news (
  
  /*	
  news_id,date formatted,ticker,author,title,link,article
  */
  `iteration` int,
  `news_id` int,
  `date formatted` string,
  `ticker` string,
  `author` string,
  `title` string,
  `link` string,
  `article` string
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ',',
  'quoteChar' = '"'
) LOCATION 's3://invest-io/sr-investio-all-dwh/news_investing/'
TBLPROPERTIES ('skip.header.line.count'='1');
