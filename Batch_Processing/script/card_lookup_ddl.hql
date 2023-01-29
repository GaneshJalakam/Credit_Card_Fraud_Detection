USE cred_financials_data;

-- Enforce Hive Bucketing
SET hive.enforce.bucketing=true;

-- Create Card Lookup Hive table (External) with HBase Integration
CREATE EXTERNAL TABLE IF NOT EXISTS card_lookup(
card_id BIGINT,
ucl DOUBLE,
postcode INT,
transaction_dt TIMESTAMP,
credit_score INT
)
CLUSTERED BY (card_id)INTO 8 BUCKETS
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping'=':key, lkp_info:ucl, lkp_info:postcode, lkp_info:transaction_dt, lkp_info:credit_score')
TBLPROPERTIES ('hbase.table.name' = 'card_lookup');

exit;
