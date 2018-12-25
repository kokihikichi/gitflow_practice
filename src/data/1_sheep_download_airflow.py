from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'bharat.prabhakar',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'email': ['bharat.prabhakar@rakuten.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('bharat_sheep_download', default_args=default_args,schedule_interval="@once")

# t1, t2, t3 and t4 are examples of tasks created using operators

dl_cmd = """hive.c4000 --hiveconf start_date=\"\'{}\'\" --hiveconf end_date=\"\'{}\'\" -f /home/prabhakarbha01/sheep/src/data/1_insert_into_download.sql"""

dl_reset_cmd = """hive.c4000 -f /home/prabhakarbha01/sheep/src/data/1_drop_create_download.sql"""



dl_reset = BashOperator(
    task_id='reset_agg_table',
    bash_command=dl_reset_cmd,
    dag=dag)

dl_10_01 = BashOperator(
    task_id='dl_10_01',
    bash_command=dl_cmd.format("2018-10-01", "2018-10-08"),
    dag=dag)

dl_10_02 = BashOperator(
    task_id='dl_10_02',
    bash_command=dl_cmd.format("2018-10-08", "2018-10-16"),
    dag=dag)

dl_10_03 = BashOperator(
    task_id='dl_10_03',
    bash_command=dl_cmd.format("2018-10-16", "2018-10-24"),
    dag=dag)

dl_10_04 = BashOperator(
    task_id='dl_10_04',
    bash_command=dl_cmd.format("2018-10-24", "2018-11-01"),
    dag=dag)

dl_10_done = BashOperator(
    task_id='dl_10_done',
    bash_command="""echo hello""",
    dag=dag)

dl_09_01 = BashOperator(
    task_id='dl_09_01',
    bash_command=dl_cmd.format("2018-09-01", "2018-09-08"),
    dag=dag)

dl_09_02 = BashOperator(
    task_id='dl_09_02',
    bash_command=dl_cmd.format("2018-09-08", "2018-09-16"),
    dag=dag)

dl_09_03 = BashOperator(
    task_id='dl_09_03',
    bash_command=dl_cmd.format("2018-09-16", "2018-09-24"),
    dag=dag)

dl_09_04 = BashOperator(
    task_id='dl_09_04',
    bash_command=dl_cmd.format("2018-09-24", "2018-10-01"),
    dag=dag)

dl_09_done = BashOperator(
    task_id='dl_09_done',
    bash_command="""echo hello""",
    dag=dag)

dl_08_01 = BashOperator(
    task_id='dl_08_01',
    bash_command=dl_cmd.format("2018-08-01", "2018-08-08"),
    dag=dag)

dl_08_02 = BashOperator(
    task_id='dl_08_02',
    bash_command=dl_cmd.format("2018-08-08", "2018-08-16"),
    dag=dag)

dl_08_03 = BashOperator(
    task_id='dl_08_03',
    bash_command=dl_cmd.format("2018-08-16", "2018-08-24"),
    dag=dag)

dl_08_04 = BashOperator(
    task_id='dl_08_04',
    bash_command=dl_cmd.format("2018-08-24", "2018-09-01"),
    dag=dag)

dl_08_done = BashOperator(
    task_id='dl_08_done',
    bash_command="""echo hello""",
    dag=dag)

dl_07_01 = BashOperator(
    task_id='dl_07_01',
    bash_command=dl_cmd.format("2018-07-01", "2018-07-08"),
    dag=dag)

dl_07_02 = BashOperator(
    task_id='dl_07_02',
    bash_command=dl_cmd.format("2018-07-08", "2018-07-16"),
    dag=dag)

dl_07_03 = BashOperator(
    task_id='dl_07_03',
    bash_command=dl_cmd.format("2018-07-16", "2018-07-24"),
    dag=dag)

dl_07_04 = BashOperator(
    task_id='dl_07_04',
    bash_command=dl_cmd.format("2018-07-24", "2018-08-01"),
    dag=dag)

dl_07_done = BashOperator(
    task_id='dl_07_done',
    bash_command="""echo hello""",
    dag=dag)




dl_10_01.set_upstream(dl_reset)
dl_10_02.set_upstream(dl_reset)
dl_10_03.set_upstream(dl_reset)
dl_10_04.set_upstream(dl_reset)
dl_10_done.set_upstream(dl_10_01)
dl_10_done.set_upstream(dl_10_02)
dl_10_done.set_upstream(dl_10_03)
dl_10_done.set_upstream(dl_10_04)

dl_09_01.set_upstream(dl_10_done)
dl_09_02.set_upstream(dl_10_done)
dl_09_03.set_upstream(dl_10_done)
dl_09_04.set_upstream(dl_10_done)
dl_09_done.set_upstream(dl_09_01)
dl_09_done.set_upstream(dl_09_02)
dl_09_done.set_upstream(dl_09_03)
dl_09_done.set_upstream(dl_09_04)

dl_08_01.set_upstream(dl_09_done)
dl_08_02.set_upstream(dl_09_done)
dl_08_03.set_upstream(dl_09_done)
dl_08_04.set_upstream(dl_09_done)
dl_08_done.set_upstream(dl_08_01)
dl_08_done.set_upstream(dl_08_02)
dl_08_done.set_upstream(dl_08_03)
dl_08_done.set_upstream(dl_08_04)

dl_07_01.set_upstream(dl_08_done)
dl_07_02.set_upstream(dl_08_done)
dl_07_03.set_upstream(dl_08_done)
dl_07_04.set_upstream(dl_08_done)
dl_07_done.set_upstream(dl_07_01)
dl_07_done.set_upstream(dl_07_02)
dl_07_done.set_upstream(dl_07_03)
dl_07_done.set_upstream(dl_07_04)
