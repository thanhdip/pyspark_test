# pyspark_test

Ingest data from a Azure blob storage with pyspark. Reads files text files in csv or json and formats it to an existing schema. 

Saves data as parquet files.

To run, have a creds.json file with neccesarry info to connect to Azure bob storage.

Run with: spark-submit --jars ./jar/hadoop-azure-3.3.0.jar,./jar/azure-storage-8.6.5.jar main.py 

Where the 2 jar files are for pyspark to connect to azure.
