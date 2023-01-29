#!/bin/bash
# Create Hive and HBase tables of Card Lookup

echo "create 'card_lookup', 'lkp_info'" | hbase shell -n

hive -f /home/hadoop/cred_financials_data/script/card_lookup_ddl.hql
