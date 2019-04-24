from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


def print_dag_id(dag__id):
    print('DAG {} is processing'.format(dag__id))


dag_id = 'another_test_dag'
dag = DAG(dag_id=dag_id,
          schedule_interval=timedelta(days=1),
          start_date=datetime(2019, 4, 22),
          description='Another test DAG',
          )

task1 = PythonOperator(
    task_id='print_dag_id',
    python_callable=print_dag_id,
    op_kwargs={'dag_id': dag_id},
    dag=dag,
)


dummy_task_1 = DummyOperator(task_id='dummy_task1', dag=dag)
dummy_task_2 = DummyOperator(task_id='dummy_task2', dag=dag)

task1 >> dummy_task_1
task1 >> dummy_task_2

# dags must be stored in global()
globals()[dag_id] = dag
