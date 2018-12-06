# -*- coding: utf-8 -*-
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

u"""
Airflow script for calc_02
"""

ALERT_MAILS = Variable.get("gv_ic_admin_lst")
DAG_NAME = str(os.path.basename(__file__).split('.')[0])
OWNER = 'User Airflow'
DEPENDS_ON_PAST = True
EMAIL_ON_FAILURE = True
EMAIL_ON_RETRY = False
RETRIES = int(Variable.get('gv_dag_retries'))
POOL = 'data_pool'
MAIN_VAR_NAME = 'gv_' + DAG_NAME

SRV_LIST = Variable.get('gv_psg_kafka_srv_list')
QUEUE_NAME = Variable.get('gv_psg_kafka_queue_name')
PARTITIONS = Variable.get('gv_psg_kafka_partitions')
LOADDTTM=str(datetime.now()).replace(" ","_")
WAIT_HRS = 1

start_dt = datetime(2018, 11, 15)

# setting default arguments of dag
default_args = {
    'owner': OWNER,
    'depends_on_past': DEPENDS_ON_PAST,
    'start_date': start_dt,
    'email': ALERT_MAILS,
    'email_on_failure': EMAIL_ON_FAILURE,
    'email_on_retry': EMAIL_ON_RETRY,
    'retries': RETRIES,
    'pool': POOL
}

# Creating DAG with parameters
dag = DAG(DAG_NAME, default_args=default_args, schedule_interval="0 */4 * * *")
dag.doc_md = __doc__

dag_start = DummyOperator(
    task_id='dag_start',
    dag=dag
)

dag_end = DummyOperator(
    task_id='dag_end',
    dag=dag
)

algo_bash_cmd = """
kinit airflow/airflow@HOME.LOCAL -kt /opt/airflow/airflow_home/kt/airflow.keytab
spark-submit --master yarn \
--num-executors {{ params.partitions }} \
--executor-cores 3 \
--executor-memory 6G \
--driver-cores 5 \
--driver-memory 10G \
--conf 'spark.driver.extraJavaOptions=-Djava.security.auth.login.config={{ params.home }}/kt/kafka_client.conf' \
--conf 'spark.executor.extraJavaOptions=-Djava.security.auth.login.config={{ params.home }}/kt/kafka_client.conf' \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.1 \
--jars """+"/opt/airflow/airflow-home/utils/HiveHomeUDF-0.0.1.jar"+""" \
{{ params.home }}/dags/pyspark/prod_data/calc_02.py {{ params.srv_list }} {{ params.queue_name }} {{ params.partitions }} {{ params.loaddttm }}
"""

algo_bash_load = BashOperator(
    task_id='prod_data_algo_calc_02',
    bash_command=algo_bash_cmd,
    execution_timeout=timedelta(hours=WAIT_HRS),
    params={
        'home': '/opt/airflow/airflow_home',
        'srv_list': SRV_LIST,
        'queue_name': QUEUE_NAME,
        'partitions': PARTITIONS,
        'loaddttm': LOADDTTM
    },
    wait_for_downstream=True,
    dag=dag
)

dag_start.set_downstream(algo_bash_load)
algo_bash_load.set_downstream(dag_end)
