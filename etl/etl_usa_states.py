#!/usr/bin/env python
# coding: utf-8


import configparser
from datetime import datetime
import os
import pandas as pd
from io import StringIO # python3; python2: BytesIO 
import boto3

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, udf, when, upper
from datetime import datetime, timedelta
import pyspark.sql.types as T


config = configparser.ConfigParser()
config.read(os.path.join(os.getcwd(), 'dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession.builder                    .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")                    .enableHiveSupport()                    .getOrCreate()
    return spark



def process_usastates(spark, input_data, output_data):
    """
    This function will read data from CSV file into Dataframe,
    then will perform some data cleaning steps.
    
    After that, will extract data into JSON file and store in S3 bucket.
    
    Following steps will be performed:
    
        - Pyspark reading CSV file into dataframe.
        - Excluding null values from dataframe.
        - Converting columns data to correct type
        - Correcting some country names to make them consitent.
        - Selecting transformed data from columns.
        - Writing final data into JSON files in S3 bucket.
    """
        
    df = spark.read.option("header", True).csv(input_data)
    
    df = exclude_null(df)
    
    
    df_final = df.select(["StateCode", "StateName"])
    df_final.write.option("header","true").csv(output_data)




def exclude_null(df):
    """
    Excluding NULL values from all columns of Dataframe.
    """
    
    return df.filter("""
                        StateCode is not null
                        AND StateName is not null
                    """)



def main():
    """
        - Create a spark session.
        - Defining path of input and output data.
        - perform ETL process by calling function process_usastates()
    """
    
    spark = create_spark_session()
    
    input_data = 'US_States.csv'
    
    output_data = "s3a://de-capstone/USA_States/US_States.csv"
    
    process_usastates(spark, input_data, output_data)


if __name__ == "__main__":
    main()

