from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 checks_dic = [{"check_sql":"", "check_expected":""}],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks_dic = checks_dic

    def execute(self, context):
        error_count = 0
        self.log.info('DataQualityOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for ch in self.checks_dic:
            result = redshift.get_records(ch.get("check_sql"))[0]
            if(result == ch.get("check_expected")):
                error_count += 1
                self.log.error("test for {} \n faill with result  =  {} expect result is {}".format(ch.get("check_sql"), result, ch.get("check_expected")))
        if(error_count > 0): 
            raise ValueError('Data quality check failed')