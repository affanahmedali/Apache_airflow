import os 
import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

#Python Functions
def process_datetime(**kwargs):
    ti = kwargs["task_instance"]
    dt = ti.xcom_pull(task_ids='get_datetime')

    if not dt:
        raise Exception("No date time value")
    
    dt = str(dt).split()
    return {
        'year': dt[-1],
        'month': dt[1],
        'day': dt[2],
        'time': dt[3],
        'day_of_week': dt[0]
    }

def save_datetime(**kwargs):
    tin = kwargs ['task_instance']
    dt_processed = tin.xcom_pull(task_ids='process_datetime')
    
    if not dt_processed:
        raise Exception('No processed date time value')
    print('working so far')
    print(dt_processed)
    print('REQUIRE SOME ADDITION')
    df = pd.DataFrame([dt_processed])
    print(df)
    
    csv_path = Variable.get('first_dag_csv_path')

    #append if file already exists else write(create new one)
    if os.path.exists(csv_path):
        df_header = False
        df_mode = 'a'
    
    else:
        df_header = True
        df_mode = 'w'

    df.to_csv(csv_path, index=False, mode=df_mode, header=df_header)

#Boiler plate code
with DAG(
    dag_id='first_airflow_dag',
    schedule='@daily',
    start_date=datetime(2024, 4, 1),
    catchup=False
) as dag:
    
    #Task 1: Get current data and time
    task_get_datetime = BashOperator(
        task_id='get_datetime',
        bash_command='date',
        dag=dag,
        do_xcom_push = True,
    )

    #Task 2: Process Current Datetime
    task_process_datetime = PythonOperator(
        task_id='process_datetime',
        python_callable=process_datetime
    )

    #Task 3: save processed date time
    task_save_datetime = PythonOperator(
        task_id = 'save_date_time',
        python_callable=save_datetime
    )

task_get_datetime >> task_process_datetime >> task_save_datetime
