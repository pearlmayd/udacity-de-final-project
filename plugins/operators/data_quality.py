from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    '''
        DataQualityOperator is created to check if tables are empty. It will also check if Demographics table population columns are always greater than 0, as it should.  
        
        If any tables are empty or population columns=0, it will raise a value error,
        and the task will fail.
        
        Otherwise, the task will completen successfully. 
        
        Define class with following attributes:
            - redshift_conn_id: Redshift connection   
    '''

    ui_color = '#89DA59'
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks =dq_checks

    def execute(self, context):
        """
            First, retrieve and initialize Redshift connection.
            Then, iterate over each item of dq_checks.
            
            If any one fails, it will be written in the log, 
            then raise a ValueError
            
            Otherwise, it will write an informational log to inform 
            the data of tables are valid and will finish the check.
        """
        
        self.log.info('Start validating data.')
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        error_count = 0
        failing_tests = []

        for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')

            records = redshift_hook.get_records(sql)[0]

            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)

        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')

        self.log.info('Data Quality checks passed!')
        
        self.log.info('Finished validating data.')