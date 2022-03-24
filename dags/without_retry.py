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

def first_function_execute(**context): # similar to Custom emr create job flow --> it is ..creating a file instead of emr cluster and saving it
    log.critical("\n############## this is critical log ########")

    print("****we are in first fucntion****")
    num = random.randint(0,10000)
    fname = f"without_retry_first {num}.txt"
    f = open(fname,"w+")
    f.close()
    print(num)
    print("File creation done")

    # filname would be in this format "without_retry_first234"



def second_function_execute(**context): # this is similar to sensor , it is just simply failing 
    print("in second fucntion")
    print(a)    # this is error -- due to which it will fail


def third_function_execute(**context): # this is a another fucntion and it is also creating a file -- we have craeted it , just to see , if other task are not getting affected by it
    num = random.randint(0,10000)
    fname = f"without_retry_third__ {num}.txt"
    f = open(fname,"w+")
    f.close()
    print("File creation done in first fucntion")
    print("third_function_execute   ")
    


def fourth_function_execute(**context): # this is subtask of third fcuntion
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

first_function_execute >> second_function_execute # this is the first task dependency

third_function_execute >> fourth_function_execute # this is the second task dependecy

'''

    this file resembles the previous state of our market data etl, were there was a simple retry 
    and it was not actually retrying

1. first fucntion will execute and it will **create a file** it will get sucesss
2. second fucntion is dependent on first fucntion and it will fail for sure - due to our error print(a)

--> now second fucnction will try to retry...but it will retry itself --> so upstream task i.e first fcuntion will not get execute 
    and no new file will be made.



problem : 
    job flow >> sensor
    job flow is not re-running after sensor fails, as only sensor is retrying not job flow so we are not seeign the time difference and no new cluster as well, when sensor fails

    first_fucntion >> second function
    -- in this dag without_retry i.e without_actual_retry 
        first fucntion is not running after second fucntion fails, we can verify this by lisitng all the files in airflow cli , so we are not able to see any new files when second fucntion fails

    what we want : 
    whenever sensor fails --> job flow should execute
    whenever second fucntion fails --> first fucntion should execute 
    
Explanation :
    mapping the analogy
    first funtion resembles creeates a file which is similar to job flow, which was creating a emr cluster on aws
    (we can verify that job flow is running by looking at emr cluster i.e new cluster should be formed)
    (we can verify that first fucntion in running by looking at new file being formed in airflow)



Solution :
    if we would be albe to see , new files in airflow cli , when second fucntion fails.
    then we can conclude that --> it is actually retrying 

    proposed solution ,
    --> clearing task instance state


deduction from this dag :
    1. cheking the airflow CLI doing ls...we are only able to see one fileby the first fcuntion
    named "without_retry_first {num}.txt"
    2. we are not seeing 2 more files ..which should be there as a result of retrying of second function
'''
