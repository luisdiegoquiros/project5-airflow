from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from helpers import SqlQueries
from stage_redshift import StageToRedshiftOperator
from load_fact import LoadFactOperator
from load_dimension import LoadDimensionOperator
from data_quality import DataQualityOperator
from create_table import CreateTableRedshiftOperator

os.environ['AWS_KEY'] = 'AKIAREQQID6FYNJMHEFC'
os.environ['AWS_SECRET'] = '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'
# export AWS_KEY=
# export AWS_SECRET=
AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'catchup': False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    redshift_conn_id='redshift',
    s3_bucket='udacity-dend/log_data',
    s3_key="{execution_date.year}/{execution_date.month}",
    table='staging_events',
    json_conf='s3://udacity-dend/log_json_path.json',
    dag=dag
)

create_table = CreateTableRedshiftOperator(
    task_id='Create_table_events',
    redshift_conn_id='redshift',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    redshift_conn_id='redshift',
    s3_bucket='udacity-dend/song_data',
    s3_key="",
    table='staging_events',
    json_conf='auto ignorecase',
    dag=dag
)

#
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.songplay_table_insert,
    dag=dag
)


load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}
    ],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_table
create_table >> stage_events_to_redshift
create_table >> stage_songs_to_redshift
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
