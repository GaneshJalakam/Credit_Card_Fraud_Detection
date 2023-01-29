USE cred_financials_data;

-- Insert the card lookup data from staging (which got prepared using Spark SQL) to bucketing table
INSERT OVERWRITE TABLE card_lookup SELECT * FROM card_lookup_stg;

-- Drop staging table
DROP TABLE card_lookup_stg;

exit;
