USE cred_financials_data;

-- Enforce Hive Bucketing
SET hive.enforce.bucketing=true;

-- Create card transactions source table and load the data from the CSV file provided (historical data)
CREATE TABLE IF NOT EXISTS card_transactions_src(
card_id BIGINT,
member_id BIGINT,
amount INT,
postcode INT,
pos_id BIGINT,
transaction_dt STRING,
status STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LOCATION '/user/hadoop/cred_financials_data/card_transactions_src'
TBLPROPERTIES ('skip.header.line.count'='1');

LOAD DATA LOCAL INPATH '/home/hadoop/cred_financials_data/data/card_transactions.csv' INTO TABLE card_transactions_src;

-- Perform type conversion for transaction date attribute and load the data in staging table
CREATE TABLE IF NOT EXISTS card_transactions_stg(
card_id BIGINT,
member_id BIGINT,
amount INT,
postcode INT,
pos_id BIGINT,
transaction_dt TIMESTAMP,
status STRING
)
location '/user/hadoop/cred_financials_data/card_transactions_stg';

INSERT INTO TABLE card_transactions_stg
SELECT card_id,
member_id, 
amount, 
postcode, 
pos_id,
CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(transaction_dt, 'dd-MM-yyyy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss')AS TIMESTAMP) AS transaction_dt,
status
FROM card_transactions_src;

-- Create Card Transactions Hive table (External) with HBase Integration
CREATE EXTERNAL TABLE IF NOT EXISTS card_transactions(
transaction_key STRING,
card_id BIGINT,
member_id BIGINT,
amount INT,
postcode INT,
pos_id BIGINT,
transaction_dt TIMESTAMP,
status STRING
)
CLUSTERED BY (card_id, member_id)INTO 16 BUCKETS
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping'=':key, transaction_data:card_id, transaction_data:member_id, transaction_data:amount, transaction_data:postcode, transaction_data:pos_id, transaction_data:transaction_dt, transaction_data:status')
TBLPROPERTIES ('hbase.table.name' = 'card_transactions');

-- Insert the staging data to Hive-HBase table
INSERT INTO TABLE card_transactions
SELECT CONCAT_WS('|', CAST(card_id AS STRING), CAST(member_id AS STRING), CAST(amount AS STRING), CAST(postcode AS STRING), CAST(pos_id AS STRING), CAST(transaction_dt AS STRING), status) AS transaction_key,
card_id,
member_id,
amount,
postcode,
pos_id,
transaction_dt,
status
FROM card_transactions_stg;

-- Drop all intermediate hive tables
DROP TABLE card_transactions_stg;
DROP TABLE card_transactions_src;

exit;
