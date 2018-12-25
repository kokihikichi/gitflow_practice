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

dag = DAG('bharat_sheep', default_args=default_args,schedule_interval="@once")

# t1, t2, t3 and t4 are examples of tasks created using operators

step1_insert_cmd = """hive.c4000 --hiveconf start_date=\"\'{}\'\" --hiveconf end_date=\"\'{}\'\" -f /home/prabhakarbha01/sheep/src/data/1_insert_into_raw.sql"""

step2_insert_cmd = """hive.c4000 --hiveconf start_date=\"\'{}\'\" --hiveconf end_date=\"\'{}\'\" -f /home/prabhakarbha01/sheep/src/data/1_insert_into_filter.sql"""

step1_reset_cmd = """hive.c4000 -f /home/prabhakarbha01/sheep/src/data/1_drop_create_raw.sql"""

step2_reset_cmd = """hive.c4000 -f /home/prabhakarbha01/sheep/src/data/1_drop_create_filter.sql"""


r1 = BashOperator(
    task_id='reset_raw',
    bash_command=step1_reset_cmd,
    dag=dag)

i1_10_01 = BashOperator(
    task_id='raw_10_1',
    bash_command=step1_insert_cmd.format("2018-10-01", "2018-10-08"),
    dag=dag)

i1_10_02 = BashOperator(
    task_id='raw_10_2',
    bash_command=step1_insert_cmd.format("2018-10-08", "2018-10-16"),
    dag=dag)

i1_10_03 = BashOperator(
    task_id='raw_10_3',
    bash_command=step1_insert_cmd.format("2018-10-16", "2018-10-24"),
    dag=dag)

i1_10_04 = BashOperator(
    task_id='raw_10_4',
    bash_command=step1_insert_cmd.format("2018-10-24", "2018-11-01"),
    dag=dag)

# i1_10_done = BashOperator(
#     task_id='raw_10_done',
#     bash_command="""echo hello""",
#     dag=dag)

i1_09_01 = BashOperator(
    task_id='raw_09_1',
    bash_command=step1_insert_cmd.format("2018-09-01", "2018-09-08"),
    dag=dag)

i1_09_02 = BashOperator(
    task_id='raw_09_2',
    bash_command=step1_insert_cmd.format("2018-09-08", "2018-09-16"),
    dag=dag)

i1_09_03 = BashOperator(
    task_id='raw_09_3',
    bash_command=step1_insert_cmd.format("2018-09-16", "2018-09-24"),
    dag=dag)

i1_09_04 = BashOperator(
    task_id='raw_09_4',
    bash_command=step1_insert_cmd.format("2018-09-24", "2018-10-01"),
    dag=dag)

# i1_09_done = BashOperator(
#     task_id='raw_09_done',
#     bash_command="""echo hello""",
#     dag=dag)

i1_08_01 = BashOperator(
    task_id='raw_08_1',
    bash_command=step1_insert_cmd.format("2018-08-01", "2018-08-08"),
    dag=dag)

i1_08_02 = BashOperator(
    task_id='raw_08_2',
    bash_command=step1_insert_cmd.format("2018-08-08", "2018-08-16"),
    dag=dag)

i1_08_03 = BashOperator(
    task_id='raw_08_3',
    bash_command=step1_insert_cmd.format("2018-08-16", "2018-08-24"),
    dag=dag)

i1_08_04 = BashOperator(
    task_id='raw_08_4',
    bash_command=step1_insert_cmd.format("2018-08-24", "2018-09-01"),
    dag=dag)

# i1_08_done = BashOperator(
#     task_id='raw_08_done',
#     bash_command="""echo hello""",
#     dag=dag)

i1_07_01 = BashOperator(
    task_id='raw_07_1',
    bash_command=step1_insert_cmd.format("2018-07-01", "2018-07-08"),
    dag=dag)

i1_07_02 = BashOperator(
    task_id='raw_07_2',
    bash_command=step1_insert_cmd.format("2018-07-08", "2018-07-16"),
    dag=dag)

i1_07_03 = BashOperator(
    task_id='raw_07_3',
    bash_command=step1_insert_cmd.format("2018-07-16", "2018-07-24"),
    dag=dag)

i1_07_04 = BashOperator(
    task_id='raw_07_4',
    bash_command=step1_insert_cmd.format("2018-07-24", "2018-08-01"),
    dag=dag)

# i1_07_done = BashOperator(
#     task_id='raw_07_done',
#     bash_command="""echo hello""",
#     dag=dag)


r2 = BashOperator(
    task_id='reset_filter',
    bash_command=step2_reset_cmd,
    dag=dag)

i2_10_01 = BashOperator(
    task_id='filter_10_1',
    bash_command=step2_insert_cmd.format("2018-10-01", "2018-10-08"),
    dag=dag)

i2_10_02 = BashOperator(
    task_id='filter_10_2',
    bash_command=step2_insert_cmd.format("2018-10-08", "2018-10-16"),
    dag=dag)

i2_10_03 = BashOperator(
    task_id='filter_10_3',
    bash_command=step2_insert_cmd.format("2018-10-16", "2018-10-24"),
    dag=dag)

i2_10_04 = BashOperator(
    task_id='filter_10_4',
    bash_command=step2_insert_cmd.format("2018-10-24", "2018-11-01"),
    dag=dag)


i2_10_done = BashOperator(
    task_id='filter_10_done',
    bash_command="""echo hello""",
    dag=dag)

i2_09_01 = BashOperator(
    task_id='filter_09_1',
    bash_command=step2_insert_cmd.format("2018-09-01", "2018-09-08"),
    dag=dag)

i2_09_02 = BashOperator(
    task_id='filter_09_2',
    bash_command=step2_insert_cmd.format("2018-09-08", "2018-09-16"),
    dag=dag)

i2_09_03 = BashOperator(
    task_id='filter_09_3',
    bash_command=step2_insert_cmd.format("2018-09-16", "2018-09-24"),
    dag=dag)

i2_09_04 = BashOperator(
    task_id='filter_09_4',
    bash_command=step2_insert_cmd.format("2018-09-24", "2018-10-01"),
    dag=dag)

i2_09_done = BashOperator(
    task_id='filter_09_done',
    bash_command="""echo hello""",
    dag=dag)

i2_08_01 = BashOperator(
    task_id='filter_08_1',
    bash_command=step2_insert_cmd.format("2018-08-01", "2018-08-08"),
    dag=dag)

i2_08_02 = BashOperator(
    task_id='filter_08_2',
    bash_command=step2_insert_cmd.format("2018-08-08", "2018-08-16"),
    dag=dag)

i2_08_03 = BashOperator(
    task_id='filter_08_3',
    bash_command=step2_insert_cmd.format("2018-08-16", "2018-08-24"),
    dag=dag)

i2_08_04 = BashOperator(
    task_id='filter_08_4',
    bash_command=step2_insert_cmd.format("2018-08-24", "2018-09-01"),
    dag=dag)

i2_08_done = BashOperator(
    task_id='filter_08_done',
    bash_command="""echo hello""",
    dag=dag)

i2_07_01 = BashOperator(
    task_id='filter_07_1',
    bash_command=step2_insert_cmd.format("2018-07-01", "2018-07-08"),
    dag=dag)

i2_07_02 = BashOperator(
    task_id='filter_07_2',
    bash_command=step2_insert_cmd.format("2018-07-08", "2018-07-16"),
    dag=dag)

i2_07_03 = BashOperator(
    task_id='filter_07_3',
    bash_command=step2_insert_cmd.format("2018-07-16", "2018-07-24"),
    dag=dag)

i2_07_04 = BashOperator(
    task_id='filter_07_4',
    bash_command=step2_insert_cmd.format("2018-07-24", "2018-08-01"),
    dag=dag)

i2_07_done = BashOperator(
    task_id='filter_07_done',
    bash_command="""echo hello""",
    dag=dag)

# r1.set_upstream(r2)
# i1_10_01.set_upstream(r1)
# i2_10_01.set_upstream(i1_10_01)
# i1_10_02.set_upstream(i2_10_01)
# i2_10_02.set_upstream(i1_10_02)
# i1_10_03.set_upstream(i2_10_02)
# i2_10_03.set_upstream(i1_10_03)
# i1_10_04.set_upstream(i2_10_03)
# i2_10_04.set_upstream(i1_10_04)

# i1_09_01.set_upstream(i2_10_04)
# i2_09_01.set_upstream(i1_09_01)
# i1_09_02.set_upstream(i2_09_01)
# i2_09_02.set_upstream(i1_09_02)
# i1_09_03.set_upstream(i2_09_02)
# i2_09_03.set_upstream(i1_09_03)
# i1_09_04.set_upstream(i2_09_03)
# i2_09_04.set_upstream(i1_09_04)

# i1_08_01.set_upstream(i2_09_04)
# i2_08_01.set_upstream(i1_08_01)
# i1_08_02.set_upstream(i2_08_01)
# i2_08_02.set_upstream(i1_08_02)
# i1_08_03.set_upstream(i2_08_02)
# i2_08_03.set_upstream(i1_08_03)
# i1_08_04.set_upstream(i2_08_03)
# i2_08_04.set_upstream(i1_08_04)

# i1_07_01.set_upstream(i2_08_04)
# i2_07_01.set_upstream(i1_07_01)
# i1_07_02.set_upstream(i2_07_01)
# i2_07_02.set_upstream(i1_07_02)
# i1_07_03.set_upstream(i2_07_02)
# i2_07_03.set_upstream(i1_07_03)
# i1_07_04.set_upstream(i2_07_03)
# i2_07_04.set_upstream(i1_07_04)


r1.set_upstream(r2)
i1_10_01.set_upstream(r1)
i1_10_02.set_upstream(r1)
i1_10_03.set_upstream(r1)
i1_10_04.set_upstream(r1)
# i1_10_done.set_upstream(i1_10_01)
# i1_10_done.set_upstream(i1_10_02)
# i1_10_done.set_upstream(i1_10_03)
i2_10_01.set_upstream(i1_10_01)
i2_10_02.set_upstream(i1_10_02)
i2_10_03.set_upstream(i1_10_03)
i2_10_04.set_upstream(i1_10_04)
i2_10_done.set_upstream(i2_10_01)
i2_10_done.set_upstream(i2_10_02)
i2_10_done.set_upstream(i2_10_03)
i2_10_done.set_upstream(i2_10_04)


i1_09_01.set_upstream(i2_10_done)
i1_09_02.set_upstream(i2_10_done)
i1_09_03.set_upstream(i2_10_done)
i1_09_04.set_upstream(i2_10_done)
# i1_09_done.set_upstream(i1_09_01)
# i1_09_done.set_upstream(i1_09_02)
# i1_09_done.set_upstream(i1_09_03)
i2_09_01.set_upstream(i1_09_01)
i2_09_02.set_upstream(i1_09_02)
i2_09_03.set_upstream(i1_09_03)
i2_09_04.set_upstream(i1_09_04)
i2_09_done.set_upstream(i2_09_01)
i2_09_done.set_upstream(i2_09_02)
i2_09_done.set_upstream(i2_09_03)
i2_09_done.set_upstream(i2_09_04)


i1_08_01.set_upstream(i2_09_done)
i1_08_02.set_upstream(i2_09_done)
i1_08_03.set_upstream(i2_09_done)
i1_08_04.set_upstream(i2_09_done)
# i1_08_done.set_upstream(i1_08_01)
# i1_08_done.set_upstream(i1_08_02)
# i1_08_done.set_upstream(i1_08_03)
i2_08_01.set_upstream(i1_08_01)
i2_08_02.set_upstream(i1_08_02)
i2_08_03.set_upstream(i1_08_03)
i2_08_04.set_upstream(i1_08_04)
i2_08_done.set_upstream(i2_08_01)
i2_08_done.set_upstream(i2_08_02)
i2_08_done.set_upstream(i2_08_03)
i2_08_done.set_upstream(i2_08_04)


i1_07_01.set_upstream(i2_08_done)
i1_07_02.set_upstream(i2_08_done)
i1_07_03.set_upstream(i2_08_done)
i1_07_04.set_upstream(i2_08_done)
# i1_07_done.set_upstream(i1_07_01)
# i1_07_done.set_upstream(i1_07_02)
# i1_07_done.set_upstream(i1_07_03)
i2_07_01.set_upstream(i1_07_01)
i2_07_02.set_upstream(i1_07_02)
i2_07_03.set_upstream(i1_07_03)
i2_07_04.set_upstream(i1_07_04)
i2_07_done.set_upstream(i2_07_01)
i2_07_done.set_upstream(i2_07_02)
i2_07_done.set_upstream(i2_07_03)
i2_07_done.set_upstream(i2_07_04)
