from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

config = {
    'TestDAG': {'schedule_interval': None, 'start_date': datetime(2019, 3, 31), 'database': 'dummy_db'}
}


def print_context(dag__id, db):
    print('{} start processing tables in database: {}'.format(dag__id, db))


for k in config:
    dag_id = '{}_{}'.format(k, config[k]['database'])
    dag = DAG(dag_id=dag_id,
              schedule_interval=config[k]['schedule_interval'],
              start_date=config[k]['start_date'],
              description='A simple test DAG {}'.format(dag_id),
              )

    print_task = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context,
        op_kwargs={'dag_id': dag_id, 'db': config[k]['database']},
        dag=dag,
    )

    insert_new_row = DummyOperator(task_id='insert_new_row', dag=dag)
    query_the_table = DummyOperator(task_id='query_the_table', dag=dag)

    print_task >> insert_new_row >> query_the_table

    # dags must be stored in global()
    globals()[dag_id] = dag
