from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class LoadFactOperator(BaseOperator):
    '''
    Load fact table in Amazon Redshift. This operator creates INSERT INTO SQL command 
    based on the parameters passed. The operator's parameters should specify name of the
    target table in Redshift and a flag to determine whether to append data into the target table.       
    '''

    ui_color = '#F98866'
    copy_sql = """
        INSERT INTO {}

    """ + SqlQueries.songplay_table_insert

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 append=True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.append = append

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append==False:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Inserting data into fact table from staging tables")
        formatted_sql = LoadFactOperator.copy_sql.format(
            self.table
        )
        redshift.run(formatted_sql)
