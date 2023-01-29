USE cred_financials_data;

-- Enforce Hive Bucketing
SET hive.enforce.bucketing=true;

-- Create member score staging table pointing to hdfs imported from MySQL
CREATE TABLE IF NOT EXISTS member_score_stg(
member_id BIGINT,
score INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/hadoop/cred_financials_data/member_score';

-- Create Hive bucketing table (External) on member score for join optimization
CREATE EXTERNAL TABLE IF NOT EXISTS member_score(
member_id BIGINT,
score INT
)
CLUSTERED BY (member_id) INTO 8 BUCKETS
STORED AS PARQUET
LOCATION '/user/hadoop/cred_financials_data/member_score_bucket';

-- Insert the member score data from staging to bucketing table
INSERT OVERWRITE TABLE member_score SELECT * FROM member_score_stg;

-- Drop staging table
DROP TABLE member_score_stg;

exit;

