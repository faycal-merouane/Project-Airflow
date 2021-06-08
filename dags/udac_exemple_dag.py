from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, PostgresOperator,)
from helpers import SqlQueries


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'email_on_retry': False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5),
    'Catchup' :False
}

dag = DAG('udac_example_dag2',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          template_searchpath = ['/home/workspace/airflow/']
        )
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
    
)
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    s3_json_path = "s3://udacity-dend/log_json_path.json",
    provide_context=True,
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    provide_context=True,
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = "public.songplays",
    append=True,
    sql =SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = "public.users",
    append=True,
    sql =SqlQueries.user_table_insert,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = "public.songs",
    append=True,
    sql =SqlQueries.song_table_insert,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = "public.artists",
    append=True,
    sql =SqlQueries.artist_table_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = "public.time",
    append=True,
    sql =SqlQueries.time_table_insert,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    dag=dag,
    checks_dic =[{"check_sql":"select count (*) from artists", "check_expected":"10025"},
                {"check_sql":"select count (*) from songplays", "check_expected":"6820"},
                {"check_sql":"select count (*) from songs", "check_expected":"14896"},
                {"check_sql":"select count (*) from time", "check_expected":"6820"},
                {"check_sql":"select count (*) from users", "check_expected":"104"}]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables

create_tables >>  stage_events_to_redshift
create_tables >>  stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

