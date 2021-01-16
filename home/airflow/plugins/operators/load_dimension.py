from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    '''
    Load dimension table in Amazon Redshift. This operator creates INSERT INTO SQL command 
    based on the parameters passed. The operator's parameters should specify name of the
    target table in Redshift, the SQL query used to extract data out of staging tables and 
    a flag to determine whether to append data into the target table.    
    '''
    
    ui_color = '#80BD9E'
    copy_sql = """
        INSERT INTO {}
        {}

    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_query="",
                 append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.select_query = select_query
        self.append=append

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append==False:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Inserting data into dimension table from staging tables")
        formatted_sql = LoadDimensionOperator.copy_sql.format(
            self.table,
            self.select_query
        )
        redshift.run(formatted_sql)
