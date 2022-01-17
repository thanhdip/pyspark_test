import sys
import os
import uuid
import json
from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType, TimestampType, IntegerType, DecimalType, StructField, StructType
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__


STORAGEACCOUNTNAME = "tdmpstorageaccount"
BLOBCONTAINERNAME = "exchangedata"

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


def common_event(*args, **kwargs):

    pass


def parse_json(line: str):
    record = json.loads(line)
    record_type = record['event_type']
    # try:
    #     # [logic to parse records]
    #     if record_type == "T":
    #         # [Get the applicable field values from json]
    #         # if key_field.empty():  # [some key fields empty]
    #             # event = common_event(col1_val, col2_val, ..., "T", "")
    #         else:
    #             # event = common_event(, , , ...., , , , , "B", line)
    #             return event
    #     elif record_type == "Q":
    #         # [Get the applicable field values from json]
    #         # if key_field.empty():  # [some key fields empty]
    #         #     event = common_event(col1_val, col2_val, ..., "Q", "")
    #         # else:
    #         #     event = common_event(, , , ...., , , , , "B", line)
    #         return event
    # except Exception as e:
    #     # [save record to dummy event in bad partition]
    #     # [fill in the fields as None or empty string]
    #     # return common_event(, , , ...., , , , , "B", line)


def parse_csv(line: str):
    record_type_pos = 2
    record = line.split(",")
    # try:
    #     # [logic to parse records]
    #     if record[record_type_pos] == "T":
    #         event = common_event(col1_val, col2_val, ..., "T", "")
    #         return event
    #     elif record[record_type_pos] == "Q":
    #         event = common_event(col1_val, col2_val, ..., "Q", "")
    #         return event
    # except Exception as e:
    #     # [save record to dummy event in bad partition]
    #     # [fill in the fields as None or empty string]
    #     return common_event(, , , ...., , , , , "B", line)


def connect_to_azure():
    creds = get_creds()
    account_name = creds["storage_account_name"]
    key = creds["storage_account_key"]
    container_name = creds["container_name"]
    account = f"fs.azure.account.key.{account_name}.blob.core.windows.net"

    spark = SparkSession.builder.master('local').appName('app').getOrCreate()
    spark.conf.set(account, key)

    # raw = spark.sparkContext.textFile(
    # f"wasbs://{container_name}@{account_name}.blob.core.windows.net/test")

    df1 = spark.read.text(
        f"wasbs://{container_name}@{account_name}.blob.core.windows.net/test.txt")

    df1.show()
    # parsed_json = raw.map(lambda line: parse_json(line))
    # data_json = spark.createDataFrame(parsed_json)

    # parsed_csv = raw.map(lambda line: parse_csv(line))
    # data_csv = spark.createDataFrame(parsed_csv)


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
