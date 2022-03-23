from email.policy import default


import logging
try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))

import time 
import random

log = logging.getLogger(__name__)




def first_function_execute(**context):
    log.critical("\n############## this is critical log ########")

    print("****we are in first fucntion****")
    my_list = [True,False]
    k = random.choice(my_list)
    raise TypeError("It is intentionally, failed")
    time.sleep(60)
    if k==False:
        raise TypeError("It is intentionally, failed")
    else:
        print("Function completed")
    context['ti'].xcom_push(key='key', value="Vishwajeet Thakur")


def second_function_execute(**context):
    print("in second fucntion starts ")
    time.sleep(30)
    instance = context["ti"]['xcom_pull']["keyiii"]
    

    print("I am in second_function_execute got value :{} from Function 1  ".format(instance))

#*/2 * * * * execute evreyt two mintes

#docker exec -it b9624c977b70a1f7a2c37e3cf2f453d794522966e33b865e51c6b5a47f65d50d /bin/sh
with DAG(
    dag_id="first_dag",
    schedule_interval="@daily",
    default_args={
        "owner":"airflow",
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
        "start_date": datetime(2022, 2, 14)
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

first_function_execute >> second_function_execute

'''
 ---> every dag is working fine
 ---> IexusSecurities # retries me no time difference : --> *******
 ---> small --> pipeline 
'''

