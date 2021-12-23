#!/usr/bin/env python
# coding: utf-8
import configparser
from datetime import datetime
import os

from pyspark.sql.functions import col, udf, when
from pyspark.sql import SparkSession
import pyspark.sql.types as T


config = configparser.ConfigParser()
config.read(os.path.join(os.getcwd(), 'dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession.builder                    .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")                    .enableHiveSupport()                    .getOrCreate()
    return spark


def process_airports(spark, input_data, output_data):
    """
    This function will read data from CSV file into Dataframe,
    then will perform some data cleaning steps.
    
    After that, will extract data into JSON file and store in S3 bucket.
    
    Following steps will be performed:
    
        - Pyspark reading CSV file into dataframe.
        - Excluding invalid values which are NULL or useless data from dataframe.
        - Fix inconsistent country code issue.
        - Converting columns data to correct type
        - Selecting transformed data from columns.
        - Writing final data into JSON files in S3 bucket.
    """
        
    df = spark.read.option("header", True).csv(input_data)
    
    df = exclude_invalid(df)
    
    df = exclude_null(df)
    
    df = update_country_code(df)
    
    df = transform_data_col(df)  
    
    
    df_airport = df.selectExpr(["ident as id", "type", "name", "elevation_ft",                                 "iso_country as country", "iso_region as region",                                "latitudes", "longitudes"])
    
    df_airport.write.json(output_data)


def exclude_null(df):
    """
    Excluding NULL values from all columns of Dataframe.
    """
    
    return df.filter("""
                        ident is not null
                        AND type is not null
                        AND name is not null
                        AND elevation_ft is not null
                        AND iso_country is not null
                        AND iso_region is not null
                        AND coordinates is not null
                    """)


def get_latitudes(x):
    try:
        lat = x.split(",")[0]
        if "-" in lat:
            return lat.replace("-", "") + "S"
        else:
            return lat + "N"
    except:
        return None


def get_longitudes(x):
    try:
        lon = x.split(",")[1]
        if "-" in lon:
            return lon.replace("-", "") + "W"
        else:
            return lon + "E"
    except:
        return None

    
def exclude_invalid(df):
    """
    Excluding NULL values from all columns of Dataframe.
    Also excluding records with invalid country codes below.
    """
    
    return df.filter(col("elevation_ft").isNotNull() & 
                    col("municipality").isNotNull() &                            
                    col("gps_code").isNotNull() &
                    col("iso_country").isin(['UM', 'MQ', 'GP', 'NF', 'BQ', 'GF']) == False)


udf_get_latitudes = udf(lambda x: get_latitudes(x))

udf_get_longitudes = udf(lambda x: get_longitudes(x))


def transform_data_col(df):
    """
        - from column 'coordinates', extract latitude values
        - from column 'coordinates', extract longitude values
        - convert data of column 'elevation_ft' from String to Integer  
    """
    
    return df.withColumn("latitudes", udf_get_latitudes(col("coordinates")))            .withColumn("longitudes", udf_get_longitudes(col("coordinates")))            .withColumn("elevation_ft", col("elevation_ft").cast(T.IntegerType()))


def update_country_code(df):
    """
     Both codes 'CD' and 'CG' represent for 'CONGO', we will just use only 'CG'.
        
    """
    return df.withColumn("iso_country", when(col("iso_country") == "CD", "CG").                                    otherwise(col("iso_country")))



def main():
    """
        - Create a spark session.
        - Defining path of input and output data.
        - perform ETL process by calling function process_airports()
    """
    
    spark = create_spark_session()
    
    input_data = 'airport-codes_csv.csv'
    
    output_data = "s3a://de-capstone/airport_codes/output/"
    
    process_airports(spark, input_data, output_data)


if __name__ == "__main__":
    main()

