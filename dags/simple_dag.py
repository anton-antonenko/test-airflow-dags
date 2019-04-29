from datetime import datetime
from datetime import timedelta

from airflow import DAG, AirflowException
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.sensors.http_sensor import HttpSensor

ssh_hook = SSHHook(remote_host='', username='', password='')


class SSHFileSensorOperator(BaseSensorOperator):
    template_fields = ('filepath',)
    ui_color = '#91818a'

    def __init__(self, filepath, task_id, poke_interval, timeout, ds):
        super(SSHFileSensorOperator, self).__init__(poke_interval=poke_interval, timeout=timeout, task_id=task_id)
        self.filepath = filepath
        self.ds = ds

    def poke(self, context):
        self.log.info('Date {}'.format(self.ds))
        self.log.info('Run ID {}'.format("{{ run_id }}"))
        self.log.info('Poking for file {}'.format(self.filepath))
        try:
            magic_word = "true"
            shell_command = '[[ -f "{}" ]] && echo "{}"'.format(self.filepath, magic_word)
            poke_result = SSHOperator(task_id="file_sensor_poke", dag=self.dag, ssh_hook=ssh_hook,
                                      command=shell_command, do_xcom_push=True).execute(context)
            response = poke_result.decode().strip("\n")
            return response == magic_word
        except AirflowException as e:
            self.log.info('Epic fail: {}'.format(str(e)))
            return False


class EmptyFileCheckOperator(SSHOperator):
    template_fields = ('filepath',)
    ui_color = '#49b0ff'

    def __init__(self, task_id, filepath, timeout):
        super(EmptyFileCheckOperator, self).__init__(
            ssh_hook=ssh_hook,
            command='[[ -s {} ]] && echo "1" || echo "0"'.format(filepath),
            do_xcom_push=True, task_id=task_id,
            timeout=timeout
        )
        self.ssh_hook = ssh_hook
        self.timeout = timeout
        self.filepath = filepath


class SparkJobOperator(SSHOperator):
    ui_color = '#ff0000'

    def __init__(self,
                 task_id,
                 class_name,
                 jar_path,
                 timeout):
        self.job_id = 'runner_{}'.format(datetime.isoformat(datetime.now()))
        super(SparkJobOperator, self).__init__(
            ssh_hook=ssh_hook,
            command='SPARK_LOCAL_IP="127.0.0.1" /usr/local/bin/spark-submit --class {} --master local[*] {}'.format(
                class_name, jar_path),
            task_id=task_id,
            do_xcom_push=True,
            timeout=timeout
        )
        self.ssh_hook = ssh_hook
        self.timeout = timeout

    def execute(self, context):
        super().execute(context)
        return self.job_id


class MyEmailOperator(EmailOperator):

    def __init__(
            self,
            task_id,
            to,
            subject,
            base_content):
        super(MyEmailOperator, self).__init__(to=to, subject=subject, html_content='', task_id=task_id)
        self.to = to
        self.subject = subject
        self.base_content = base_content

    def execute(self, context):
        job_id = context['task_instance'].xcom_pull(task_ids='run_job')
        self.html_content = 'Email test {}'.format(job_id)
        self.log.info('Content: {}'.format(self.html_content))
        super().execute(context)


class HBaseTestOperator(HttpSensor):

    def __init__(self,
                 task_id,
                 http_conn_id,
                 endpoint,
                 response_check,
                 poke_interval,
                 timeout
                 ):
        super(HBaseTestOperator, self).__init__(poke_interval=poke_interval, timeout=timeout, endpoint=endpoint,
                                                http_conn_id=http_conn_id, response_check=response_check, task_id=task_id)
        self.method = 'GET'
        self.endpoint = endpoint
        self.response_check = response_check

    def poke(self, context):
        try:
            return super().poke(context)
        except Exception as e:
            self.log.info('HBase poke: {}'.format(str(e)))
            return False


def check_me(response):
    return response.ok


default_args = {'retries': 3, 'retry_delay': timedelta(seconds=10)}

with DAG(dag_id='My_dag', schedule_interval=None, start_date=datetime(2019, 3, 31), default_args=default_args,
         description='A simple test DAG') as dag:
    filepath = '/test/test.csv'

    file_sensor = SSHFileSensorOperator(task_id='sensor', filepath=filepath, poke_interval=10, timeout=30, ds='{{ ds }}')

    empty_check = EmptyFileCheckOperator(task_id='empty_file_check', filepath=filepath, timeout=10)

    ssh_task = SparkJobOperator(class_name='com.gd.Runner',
                                jar_path='spark-assembly-0.1.jar',
                                task_id='run_job', timeout=300)

    test_operator = HBaseTestOperator(task_id="test_metadata", http_conn_id='simple_http', endpoint='/',
                                      response_check=check_me, poke_interval=10, timeout=60)

    compose_email = MyEmailOperator(task_id='email', to='me@example.com', subject='Test email',
                                    base_content='Email test')

    file_sensor >> empty_check >> ssh_task >> test_operator >> compose_email
