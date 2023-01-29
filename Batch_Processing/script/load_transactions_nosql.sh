#!/bin/bash
# Create Hive and HBase tables for card transactions historical data and load data in it

echo "create 'card_transactions', 'transaction_data'" | hbase shell -n

hive -f /home/hadoop/cred_financials_data/script/card_transactions_history.hql
