from email.policy import default


import logging
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.models import DAG, taskinstance

from airflow.utils.state import State
from airflow.utils.db import provide_session

import time 
import random

print("All Dag modules are ok ......")

log = logging.getLogger(__name__)

print("******** its runing *********")

def first_function_execute(**context):
    log.critical("\n############## this is critical log ########")

    print("****we are in first fucntion****")
    num = random.randint(0,10000)
    fname = f"without_retry_first {num}.txt"
    f = open(fname,"w+")
    f.close()
    print(num)
    print("File creation done")



def second_function_execute(**context):
    print("in second fucntion")
    print(a)


def third_function_execute(**context):
    num = random.randint(0,10000)
    fname = f"without_retry_third__ {num}.txt"
    f = open(fname,"w+")
    f.close()
    print("File creation done in first fucntion")
    print("third_function_execute   ")
    


def fourth_function_execute(**context):
    print("fourth fucntion") 
#*/2 * * * * execute evreyt two mintes

#docker exec -it container_id /bin/sh
with DAG(
    dag_id="Without_retry_dag",
    schedule_interval="@daily",
    default_args={
        "owner":"airflow",
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
        "start_date": datetime(2022, 3, 21)
    },
    catchup=False) as f:


    first_function_execute = PythonOperator(
        task_id = "first_function_execute",
        python_callable=first_function_execute,
        provide_context=True,
        retries= 2,
    )

    second_function_execute = PythonOperator(
        task_id="second_function_execute",
        python_callable=second_function_execute,
        provide_context=True,
        retries= 2,

    )

    third_function_execute = PythonOperator(
        task_id = "third_function_execute",
        python_callable=third_function_execute,
        provide_context=True,
        retries= 2,
    )

    fourth_function_execute = PythonOperator(
        task_id="fourth_function_execute",
        python_callable=fourth_function_execute,
        provide_context=True,
        retries= 2,
    )

first_function_execute >> second_function_execute

third_function_execute >> fourth_function_execute

'''
 ---> every dag is working fine
 ---> IexusSecurities # retries me no time difference : --> *******
 ---> small --> pipeline 
'''
