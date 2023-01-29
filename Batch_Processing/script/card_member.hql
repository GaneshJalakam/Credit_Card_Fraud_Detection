USE cred_financials_data;

-- Enforce Hive Bucketing
SET hive.enforce.bucketing=true;

-- Create card member staging table pointing to hdfs imported from MySQL
CREATE TABLE IF NOT EXISTS card_member_stg(
card_id BIGINT,
member_id BIGINT,
member_joining_dt TIMESTAMP,
card_purchase_dt STRING,
country STRING,
city STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/hadoop/cred_financials_data/card_member';

-- Create Hive bucketing table (External) on card member for join optimization
CREATE EXTERNAL TABLE IF NOT EXISTS card_member(
card_id BIGINT,
member_id BIGINT,
member_joining_dt TIMESTAMP,
card_purchase_dt STRING,
country STRING,
city STRING
)
CLUSTERED BY (card_id, member_id) INTO 8 BUCKETS
STORED AS PARQUET
LOCATION '/user/hadoop/cred_financials_data/card_member_bucket';

-- Insert the card member data from staging to bucketing table
INSERT OVERWRITE TABLE card_member SELECT * FROM card_member_stg;

-- Drop staging table
DROP TABLE card_member_stg;

exit;
