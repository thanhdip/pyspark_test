import sys
import os
import uuid
import json
from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType, TimestampType, IntegerType, DecimalType, StructField, StructType
from datetime import date, datetime
from decimal import Decimal
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


# Schema combine of 2 type of data. Trade and quote

# Arrival Timestamp Timestamp - Created when file put into staging
# Partition

# TRADE:
# Trade Date Date
# Record Type Varchar(1)
# Symbol String
# Execution ID String
# Event Time Timestamp
# Event Sequence Number Int
# Exchange String
# Trade Price Decimal
# Trade Size Int - NOT used

# QUOTE:
# Trade Date Date
# Record Type Varchar(1)
# Symbol String
# Event Time Timestamp
# Event Sequence
# Number
# Int
# Exchange String
# Bid Price Decimal
# Bid Size Int
# Ask Price Decimal
# Ask Size Int

SCHEMA = StructType([
    StructField("trade_dt", DateType(), True),
    StructField("rec_type", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("event_tm", TimestampType(), True),
    StructField("event_seq_nb", IntegerType(), True),
    StructField("arrival_tm", TimestampType(), True),
    StructField("trade_pr", DecimalType(), True),
    StructField("bid_pr", DecimalType(), True),
    StructField("bid_size", IntegerType(), True),
    StructField("ask_pr", DecimalType(), True),
    StructField("ask_size", IntegerType(), True),
    StructField("partition", StringType(), True),

])


def common_event(l, record):
    ret = []
    for accessor in l:
        if accessor == "":
            ret.append("")
        elif accessor == "T":
            ret.append("T")
        elif accessor == "Q":
            ret.append("Q")
        else:
            ret.append(record[accessor])
    return tuple(ret)


def parse_csv(line):
    line = line.value
    record_type_pos = 2
    record = line.split(",")
    try:
        # [logic to parse records]
        if record[record_type_pos] == "T":
            return common_event([0,     # Trade Date
                                 2,     # rec_type
                                 3,     # symbol
                                 6,     # exchange
                                 4,     # event_tm
                                 5,     # event_seq_nb
                                 1,     # arrival_tm
                                 7,     # trade_pr
                                 "",    # bid_pr
                                 "",    # bid_size
                                 "",    # ask_pr
                                 "",    # ask_size
                                 "T"    # partition
                                 ], record)
        elif record[record_type_pos] == "Q":
            return common_event([0,     # Trade Date
                                 2,     # rec_type
                                 3,     # symbol
                                 6,     # exchange
                                 4,     # event_tm
                                 5,     # event_seq_nb
                                 1,     # arrival_tm
                                 "",    # trade_pr
                                 7,     # bid_pr
                                 8,     # bid_size
                                 9,     # ask_pr
                                 10,    # ask_size
                                 "Q",   # partition
                                 ], record)

    except Exception as e:
        # [save record to dummy event in bad partition]
        # [fill in the fields as None or empty string]
        return event_dummy()


def parse_json(line):
    # Help function for making sure all keys in json is present.
    def has_all_fields(expected, current):
        return set(expected).issubset(set(current))

    line = line.value
    record = json.loads(line)
    record_type = record['event_type']
    cur_keys = record.keys()
    try:
        # [logic to parse records]
        if record_type == "T":
            # [Get the applicable field values from json]
            expected_keys = ['trade_dt', 'file_tm', 'event_type', 'symbol',
                             'execution_id', 'event_tm', 'event_seq_nb', 'exchange', 'price', 'size']
            # [some key fields empty]
            if has_all_fields(expected_keys, cur_keys):
                event = common_event(['trade_dt', 'event_type', 'symbol', 'exchange', 'event_tm',
                                     'event_seq_nb', 'file_tm', 'price', '', '', '', '',  'T'], record)
            else:
                return event_dummy()

        elif record_type == "Q":
            # [Get the applicable field values from json]
            expected_keys = ['trade_dt', 'file_tm', 'event_type', 'symbol', 'event_tm',
                             'event_seq_nb', 'exchange', 'bid_pr', 'bid_size', 'ask_pr', 'ask_size']
            # [some key fields empty]
            if has_all_fields(expected_keys, cur_keys):
                event = common_event(['trade_dt', 'event_type', 'symbol', 'exchange', 'event_tm',
                                     'event_seq_nb', 'file_tm', '', 'bid_pr', 'bid_size', 'ask_pr', 'ask_size', 'Q'], record)
            else:
                return event_dummy()
        return event
    except Exception as e:
        # [save record to dummy event in bad partition]
        # [fill in the fields as None or empty string]
        print(e)
        return event_dummy()


def event_dummy():
    dummy = [""] * 12
    dummy.append("B")
    return tuple(dummy)


def connect_to_azure():
    creds = get_creds()
    account_name = creds["storage_account_name"]
    key = creds["storage_account_key"]
    container_name = creds["container_name"]
    account = f"fs.azure.account.key.{account_name}.blob.core.windows.net"

    spark = SparkSession.builder.master('local').appName('app').getOrCreate()
    spark.conf.set(account, key)

    # Read all texts files in the csv and json folders. Turn DF to RDD for map()
    csv_rdd = spark.read.text(
        f"wasbs://{container_name}@{account_name}.blob.core.windows.net/csv/").rdd
    json_rdd = spark.read.text(
        f"wasbs://{container_name}@{account_name}.blob.core.windows.net/json/").rdd

    # Map() csv rows and others
    parsed_csv_rdd = csv_rdd.map(lambda line: parse_csv(line))
    csv_df = spark.createDataFrame(data=parsed_csv_rdd, schema=SCHEMA.names)

    parsed_json_rdd = json_rdd.map(lambda line: parse_json(line))
    json_df = spark.createDataFrame(data=parsed_json_rdd, schema=SCHEMA.names)

    csv_df = cast_all_rows(csv_df, SCHEMA)
    json_df = cast_all_rows(json_df, SCHEMA)

    final_df = csv_df.union(json_df)

    data_dir = f"wasbs://{container_name}@{account_name}.blob.core.windows.net/clean/"

    final_df.write.partitionBy("partition").mode(
        "overwrite").parquet(data_dir)


def cast_all_rows(df, schema):
    for name in schema.names:
        df = df.withColumn(name, df[name].cast(schema[name].dataType))
    return df


def connect_to_container():
    creds = get_creds()
    container_url = creds["container_url"]
    try:
        container_client = ContainerClient.from_container_url(container_url)
        return container_client

    except Exception as ex:
        print('Exception:')
        print(ex)


def get_creds(fn="creds"):
    with open(fn + ".json") as file:
        cred = json.load(file)
        return cred


def main(args):
    # connect_to_container()
    connect_to_azure()


if __name__ == "__main__":
    main(sys.argv)
