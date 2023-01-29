#!/bin/bash
# Sqoop Import Card Member and Member Score data from AWS RDS to HDFS
# Execution Command: ./sqoop_data_ingestion.sh <AWS RDS Connection String> <database> <username> <password>

# AWS RDS Credentials
rds_connection=$1
database=$2
username=$3
password=$4


# Sqoop Import - Card Member table
sqoop import \
--connect jdbc:mysql://${rds_connection}/${database} \
--username ${username} \
--password ${password} \
--table card_member \
--warehouse-dir /user/hadoop/cred_financials_data \
--delete-target-dir \
--num-mappers 1 \
--compress

# Sqoop Import - Member Score table
sqoop import \
--connect jdbc:mysql://${rds_connection}/${database} \
--username ${username}  \
--password ${password} \
--table member_score \
--warehouse-dir /user/hadoop/cred_financials_data \
--delete-target-dir \
--num-mappers 1 \
--compress
