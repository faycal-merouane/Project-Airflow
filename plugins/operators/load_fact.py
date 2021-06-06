from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table = "",
                sql ="",
                append = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append = append


    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if(not self.append):
            self.log.info('LoadFactOperator append is {}'.format(self.append))
            redshift.run("DELETE FROM {}".format(self.table))
        self.log.info('LoadFactOperator insert data in : {}'.format(self.table))
        redshift.run(self.sql)