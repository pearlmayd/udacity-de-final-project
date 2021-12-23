from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionCsvOperator,
                                LoadDimensionOperator, DataQualityOperator, PostgresOperator)

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from helpers import SqlQueries
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 11, 1),
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'redshift_conn_id': 'redshift',
    'aws_credential_id': 'aws_credentials'
}

dag = DAG('udac_example_dag',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '0 7 * * *',
          max_active_runs = 6
        )

start_operator = DummyOperator(task_id='Begin_execution', default_args = default_args, dag=dag)

staging_airports = StageToRedshiftOperator(
    task_id = 'staging_airports',
    dag = dag,
    table = "staging_airports",
    s3_bucket = "s3://de-capstone/airport_codes/output/"
)

staging_immigrations = StageToRedshiftOperator(
    task_id = 'staging_immigrations',
    dag = dag,
    table = "staging_immigrations",
    s3_bucket = "s3://de-capstone/immigration/output/"
)

stage_temperatures = StageToRedshiftOperator(
    task_id='stage_temperatures',
    dag=dag,
    table = "staging_temperatures",
    s3_bucket = "s3://de-capstone/temperature/output/"
)

staging_demographics = StageToRedshiftOperator(
    task_id = 'staging_demographics',
    dag = dag,
    table = "staging_demographics",
    s3_bucket = "s3://de-capstone/demographics/output/"
)

dimension_usa_states = LoadDimensionCsvOperator(
    task_id = 'dimension_usa_states',
    dag = dag,
    table = "USAStates",
    s3_bucket = "s3://de-capstone/USA_States/US_States.csv"
)

dimension_countries = LoadDimensionCsvOperator(
    task_id = 'dimension_countries',
    dag = dag,
    table = "Countries",
    s3_bucket = "s3://de-capstone/country_data/countries.csv"
)

insert_dimension_coordinates = LoadDimensionOperator(
    task_id = 'insert_dimension_coordinates',
    dag = dag,
    insert_sql = SqlQueries.insert_dimension_coordinates
)

insert_dimension_regions = LoadDimensionOperator(
    task_id = 'insert_dimension_regions',
    dag = dag,
    insert_sql = SqlQueries.insert_dimension_regions
)

insert_dimension_cities = LoadDimensionOperator(
    task_id = 'insert_dimension_cities',
    dag = dag,
    insert_sql = SqlQueries.insert_dimension_cities
)

insert_dimension_dates = LoadDimensionOperator(
    task_id = 'insert_dimension_dates',
    dag = dag,
    insert_sql = SqlQueries.insert_dimension_dates
)

insert_dimension_flights = LoadDimensionOperator(
    task_id = 'insert_dimension_flights',
    dag = dag,
    insert_sql = SqlQueries.insert_dimension_flights
)

insert_dimension_transportations = LoadDimensionOperator(
    task_id = 'insert_dimension_transportations',
    dag = dag,
    insert_sql = SqlQueries.insert_dimension_transportations
)

insert_fact_immigrations = LoadFactOperator(
    task_id = 'insert_fact_immigrations',
    dag = dag,
    insert_sql = SqlQueries.insert_fact_immigrations
)

insert_fact_temperatures = LoadFactOperator(
    task_id = 'insert_fact_temperatures',
    dag = dag,
    insert_sql = SqlQueries.insert_fact_temperatures
)

insert_fact_demographics = LoadFactOperator(
    task_id = 'insert_fact_demographics',
    dag = dag,
    insert_sql = SqlQueries.insert_fact_demographics
)

insert_fact_airports = LoadFactOperator(
    task_id = 'insert_fact_airports',
    dag = dag,
    insert_sql = SqlQueries.insert_fact_airports
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    dq_checks=[
        {'check_sql': "SELECT CASE WHEN COUNT(*) > 0 THEN 1 END FROM Flights", 'expected_result': 1},
        {'check_sql': "SELECT CASE WHEN COUNT(*) > 0 THEN 1 END FROM TransportationModes", 'expected_result': 1},
        {'check_sql': "SELECT CASE WHEN COUNT(*) > 0 THEN 1 END FROM Countries", 'expected_result': 1},
        {'check_sql': "SELECT CASE WHEN COUNT(*) > 0 THEN 1 END FROM Dates", 'expected_result': 1},
        {'check_sql': "SELECT CASE WHEN COUNT(*) > 0 THEN 1 END FROM USAStates", 'expected_result': 1},
        {'check_sql': "SELECT CASE WHEN COUNT(*) > 0 THEN 1 END FROM Cities", 'expected_result': 1},
        {'check_sql': "SELECT CASE WHEN COUNT(*) > 0 THEN 1 END FROM Coordinates", 'expected_result': 1},
        {'check_sql': "SELECT CASE WHEN COUNT(*) > 0 THEN 1 END FROM Regions", 'expected_result': 1},
        {'check_sql': "SELECT CASE WHEN COUNT(*) > 0 THEN 1 END FROM Immigrations", 'expected_result': 1},
        {'check_sql': "SELECT CASE WHEN COUNT(*) > 0 THEN 1 END FROM Temperatures", 'expected_result': 1},
        {'check_sql': "SELECT CASE WHEN COUNT(*) > 0 THEN 1 END FROM Demographics", 'expected_result': 1},
        {'check_sql': "SELECT CASE WHEN COUNT(*) > 0 THEN 1 END FROM Airports", 'expected_result': 1},
        {'check_sql': "SELECT COUNT(1) FROM Demographics WHERE femalepopulation <= 0 OR malepopulation <= 0 OR numberveterans <= 0 OR averagehouseholdsize <= 0 OR foreignborn <= 0 OR medianage <= 0", 'expected_result': 0}
    ],
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

"""
There are 4 sets of tasks:
- Set1: tasks to load JSON files from S3 to staging tables in Redshift.
- Set2: tasks to load external datasets (CSV) from S3 to 2 dimension tables COuntries & USAStates
- Set3: tasks to query data from staging tables. then adding into dimension tables.
- Set4: tasks to query data from both staging & dimension tables, then adding into fact table.
Dependencies between group tasks should be:
Set1 >> Set2 >> Set3 >> Set4
"""

# Set1
s3_to_staging = (stage_temperatures, staging_demographics, staging_airports, staging_immigrations)

# Set2
external_ds_to_dimension = (dimension_countries, dimension_usa_states)

# Set3
insert_dimensions = (insert_dimension_coordinates, insert_dimension_regions, insert_dimension_cities, insert_dimension_dates, insert_dimension_flights, insert_dimension_transportations)

# Set4
insert_facts = (insert_fact_airports, insert_fact_demographics, insert_fact_temperatures, insert_fact_immigrations)

start_operator >> s3_to_staging

stage_temperatures >> external_ds_to_dimension
staging_demographics >> external_ds_to_dimension
staging_airports >> external_ds_to_dimension
staging_immigrations >> external_ds_to_dimension


dimension_countries >> insert_dimensions
dimension_usa_states >> insert_dimensions

insert_dimensions >> insert_fact_airports
insert_dimensions >> insert_fact_demographics
insert_dimensions >> insert_fact_temperatures
insert_dimensions >> insert_fact_immigrations

insert_facts >> run_quality_checks
run_quality_checks >> end_operator