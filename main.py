from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from tasks import merge_data, preprocess_data, create_model


dag = DAG(
    'Reddit Topic Modeling', 
    description='All /r/stashinvest and top 1000 /r/personalfinance posts modeled using a GSDMM model',
    schedule_interval='None',
    start_date=datetime.now(), 
    catchup=False
)

merge_data_operator = PythonOperator(
    task_id='merge_data', 
    python_callable=merge_data.run, 
    dag=dag
)

preprocess_data_operator = PythonOperator(
    task_id='merge_data', 
    python_callable=preprocess_data.run, 
    dag=dag
)

create_model_operator = PythonOperator(
    task_id='create_model', 
    python_callable=create_model.run, 
    dag=dag
)

merge_data_operator >> preprocess_data_operator >> create_model_operator