#!/bin/bash
# Prepare card lookup data using Spark SQL and load it to card lookup Hive and HBase tables

spark-submit /home/hadoop/cred_financials_data/script/card_lookup_preprocessing.py

hive -f /home/hadoop/cred_financials_data/script/card_lookup_insert.hql
