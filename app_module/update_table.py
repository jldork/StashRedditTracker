import argparse
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession

def run(env):
    with Timer('Updating Table with New Partition'):
        spark_session = SparkSession.builder.appName("reddit").enableHiveSupport().getOrCreate()
        spark_session.sparkContext.setLogLevel("WARN")
        spark_session.sql(f"""
        ALTER TABLE source_social.reddit_scores
        ADD IF NOT EXISTS PARTITION(process_date='{datetime.today().strftime("%Y-%m-%d")}')
        LOCATION "s3://stash-de-source-{env}/source_social.db/reddit_scores/process_date={datetime.today().strftime("%Y-%m-%d")}";
        """)
    
if __name__=='__main__':
    parser = argparse.ArgumentParser(
        description='Point Hive Table to Parquet File')
    parser.add_argument(
        'env', type=str, help='Environment to create Hive tables and upload data:\n\n\tvalid values: edge | prod')
    args = parser.parse_args()
    run(args.env)
