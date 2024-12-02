import requests
import json
from datetime import datetime

#from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
from airflow.decorators import dag, task
import pendulum

local_tz = pendulum.timezone("Asia/Bangkok")

default_args = {
    'owner': 'Ekarat',
    'start_date': datetime(2021, 2, 28, 0, 0, 0, tzinfo=local_tz)
}

@dag('line-notify',
        schedule_interval='0 0-23/1 * * *',
        default_args=default_args,
        description='A simple data pipeline for line-notify',
        catchup=False)
def cline_dag():

    @task(task_id='get_bitcoin')
    def get_bitcoin_report_today():
        url = 'https://api.binance.com/api/v3/ticker?type=MINI&symbol=BTCUSDT&windowSize=1h'
        response = requests.get(url)
        data = response.json()
        return data

    @task(task_id='send_line')
    def send_line_notify(data):
        url = 'https://notify-api.line.me/api/notify'
        token = 'PL4ngWSwLdJITTTfTTRF36lM0bLK8nUavkP6J4sYqBK'
        headers = {
            'content-type':
            'application/x-www-form-urlencoded',
            'Authorization': 'Bearer '+ token
        }

        msg = data['lastPrice'] + "\n"
        msg += str(float(data['lastPrice']) * 36.0) + "\n"
        r = requests.post(url, headers=headers, data={'message': msg})
        #print(r.text)

    send_line_notify(get_bitcoin_report_today())

cline_dag()
