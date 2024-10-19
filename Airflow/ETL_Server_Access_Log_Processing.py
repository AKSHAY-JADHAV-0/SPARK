#import modules fr
from datetime import timedelta
from airflow.modules import DAG
from airflow.operator.bash.operator import Bashoperator
from airflow.utils.operator import day_ago

defaults_arg = {
    'owner':'akshay jadhav'
    'start_date':'day_ago'
    'email':'jadhavakshay0504@gmail.com'
    'retries': 1,
    'delay_retries':timedelta(minutes=5)
}

dag = DAG(
    'my_first_dag'
    defaults_arg=defaults_arg
    description='my_secomd_dag'
    scedule_interval=timedelta(day=1)
)

    Kf