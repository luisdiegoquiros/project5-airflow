from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTableRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 *args, **kwargs):
        super(CreateTableRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        staging_events_table_create = ("""
        CREATE TABLE  IF NOT EXISTS "dev"."public"."staging_events" (
            artist varchar,
            auth varchar,
            firstName varchar,
            gender varchar,
            itemInSession integer,
            lastName varchar,
            length REAL 	,
            level varchar,
            location varchar,
            method varchar,
            page varchar,
            registration REAL ,
            sessionId integer ,
            song varchar,
            status varchar, 
            ts bigint  ,
            userAgent varchar,
            userId varchar)
            DISTSTYLE EVEN
        """)

        staging_songs_table_create = ("""
        CREATE TABLE  IF NOT EXISTS "dev"."public"."staging_songs" (
            artist_id varchar,
            artist_latitude REAL ,
            artist_location varchar,
            artist_longitude REAL,
            artist_name varchar,
            duration REAL	,
            num_songs integer 	,
            song_id varchar,
            title varchar,
            year integer,
            primary key(artist_id))
            DISTSTYLE EVEN
        """)

        self.log.info('Create tables in redshift')

        self.log.info('Creating Staging Events')
        redshift.run(staging_events_table_create)

        self.log.info('Creating Song')
        redshift.run(staging_songs_table_create)

        self.log.info('Finish table creation')
