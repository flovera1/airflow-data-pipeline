from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import EmptyOperator 
from airflow.models import Variable


from airflow.plugins.final_project_operators import StageToRedshiftOperator
from airflow.plugins.final_project_operators import LoadFactOperator
from airflow.plugins.final_project_operators import LoadDimensionOperator
from airflow.plugins.final_project_operators import DataQualityOperator


from udacity.common.final_project_sql_statements import SqlQueries

S3_BUCKET = Variable.get("s3_bucket", default_var="")
LOG_JSON_PATH = Variable.get("log_json_path", default_var="")

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG(
    'udacity_final_project',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
)

start_operator = EmptyOperator(task_id='Begin_execution', dag=dag)

stage_events = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket=S3_BUCKET,
    s3_key='log-data/{execution_date.year}/{execution_date.month}',
    json_path=LOG_JSON_PATH,
    dag=dag
)

stage_songs = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket=S3_BUCKET,
    s3_key='song-data',
    json_path='auto',
    dag=dag
)

load_songplays = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    table='songplays',
    sql=SqlQueries.songplay_table_insert,
    dag=dag
)

load_users = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    table='users',
    sql=SqlQueries.user_table_insert,
    mode='truncate-insert',
    dag=dag
)

load_songs = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    table='songs',
    sql=SqlQueries.song_table_insert,
    mode='truncate-insert',
    dag=dag
)

load_artists = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    table='artists',
    sql=SqlQueries.artist_table_insert,
    mode='truncate-insert',
    dag=dag
)

load_time = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    table='time',
    sql=SqlQueries.time_table_insert,
    mode='truncate-insert',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    tests=[
        {"check_sql": "SELECT COUNT(*) FROM songplays", "expected_result": 0, "comparison": "greater_than"},
        {"check_sql": "SELECT COUNT(*) FROM users WHERE userid IS NULL", "expected_result": 0},
        {"check_sql": "SELECT COUNT(*) FROM songs WHERE song_id IS NULL", "expected_result": 0},
        {"check_sql": "SELECT COUNT(DISTINCT songplay_id) FROM songplays", "expected_result": 0, "comparison": "greater_than"}
    ],
    dag=dag
)

end_operator = EmptyOperator(task_id='Stop_execution', dag=dag)

start_operator >> [stage_events, stage_songs]
[stage_events, stage_songs] >> load_songplays
load_songplays >> [load_users, load_songs, load_artists, load_time]
[load_users, load_songs, load_artists, load_time] >> run_quality_checks
run_quality_checks >> end_operator
