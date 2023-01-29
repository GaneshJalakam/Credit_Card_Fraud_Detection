# Import necessary PySpark libraries
import pyspark
from pyspark.sql import SparkSession

# Create a Spark Session with Hive support
spark = SparkSession \
        .builder \
        .appName('Credit Card Lookup data preparation') \
        .enableHiveSupport() \
        .getOrCreate()
        
# Set log level to ERROR
spark.sparkContext.setLogLevel('ERROR')

# Prepare the card lookup data from Card Transactions and Member Score Hive tables using Spark SQL and store the results in a temporary view
spark.sql('''
WITH transaction_details AS
(
    SELECT
        card_id,
        member_id,
        amount,
        postcode,
        transaction_dt,
        RANK() OVER(PARTITION BY card_id ORDER BY transaction_dt DESC) AS txn_rank
    FROM
        cred_financials_data.card_transactions
)
SELECT
    card_id,
    ROUND(AVG(amount) + 3 *MAX(std_dev), 0) AS ucl,
    FIRST_VALUE(postcode) OVER(PARTITION BY card_id ORDER BY (SELECT 1)) AS postcode,
    MAX(transaction_dt) AS transaction_dt,
    credit_score
FROM
    (
        SELECT
            txn.card_id,
            txn.amount,
            FIRST_VALUE(txn.postcode) OVER(PARTITION BY card_id ORDER BY txn.txn_rank) AS postcode,
            txn.transaction_dt,
            mem.score as credit_score,
            ROUND(STDDEV(txn.amount) OVER(PARTITION BY card_id ORDER BY (SELECT 1)), 0) AS std_dev
        FROM
            transaction_details txn
            INNER JOIN cred_financials_data.member_score mem
                ON txn.member_id = mem.member_id
        WHERE
            txn.txn_rank <= 10
    ) a
GROUP BY
    card_id, 
    postcode,
    credit_score
''').createOrReplaceTempView('card_lookup_tmp')


# Create a staging table in Hive and load the data from temporary view
spark.sql('CREATE TABLE cred_financials_data.card_lookup_stg AS SELECT * FROM card_lookup_tmp')

# Drop temporary view to release the memory
spark.catalog.dropTempView('card_lookup_tmp')

spark.stop()
