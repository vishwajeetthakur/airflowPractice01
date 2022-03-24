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
@provide_session
def clear_task(task_id, session = None, adr = False, dag = None):
    print("in clear task******")
    print(task_id)
    taskinstance.clear_task_instances(tis = task_id,
                                      session = session,
                                      activate_dag_runs = adr,
                                      dag = dag)
    print("********************** clear task is completed ")

def retry_upstream_task(context):

    print("% Retry upstream is called ")
    tasks = context["dag_run"].get_task_instances()
    dag = context["dag"]
    to_retry = context["params"].get("to_retry", [])

    task_to_retry = [ ti for ti in tasks if ti.task_id in to_retry ]

    clear_task(task_to_retry, dag = dag)
    print("% ** Retry upstream is completed ")



def first_function_execute(**context):
    log.critical("\n############## this is critical log ########")

    print("****we are in first fucntion****")
    num = random.randint(0,10000)
    fname = f"first {num}.txt"
    f = open(fname,"w+")
    f.close()
    print(num)
    print("File creation done")



def second_function_execute(**context):
    print("in second fucntion")
    print(a)


def third_function_execute(**context):
    num = random.randint(0,10000)
    fname = f"third__ {num}.txt"
    f = open(fname,"w+")
    f.close()
    print("File creation done in first fucntion")
    print("third_function_execute   ")
    


def fourth_function_execute(**context):
    print("fourth fucntion") 
#*/2 * * * * execute evreyt two mintes

#docker exec -it b9624c977b70a1f7a2c37e3cf2f453d794522966e33b865e51c6b5a47f65d50d /bin/sh
with DAG(
    dag_id="first_dag",
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
        on_retry_callback=retry_upstream_task,
        params = {"to_retry":['first_function_execute']}
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
what i have done
-- applied retry logic by clearing task instance state

deduction from this dag :
    1. cheking the airflow CLI doing ls...now we are able to see 3 files named 
    " first {num}.txt "

Conclusion:
    when this ran
    first fucntion >> second fucntion

    1. first file formed due to ....first fucntion in the very first run of first fucntion
    2. second file formed due to ...when second fucntion executes and get failed first time...first fucntion state got cleared and new file is foremed due t0 first fucntion
    3. third file formed due to ....when second fucntion executes and get failed second time...first fucntion state got cleared and new file is foremed due t0 first fucntion

    So we can say this approach is working fine and doing actual retries.
'''
