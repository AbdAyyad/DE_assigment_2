from datetime import timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging

import pandas as pd
import json

LOGGER = logging.getLogger("airflow.task")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
        'assignment_2',
        default_args=default_args,
        description='data engineering assignment #2',
        schedule_interval=None,
        start_date=days_ago(2),
        tags=['assignment'],
) as dag:
    def Get_DF_i(Day):
        DF_i = None
        try:
            URL_Day = f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{Day}.csv'
            DF_day = pd.read_csv(URL_Day)
            DF_day['Day'] = Day
            cond = (DF_day.Country_Region == 'India') & (DF_day.Province_State == 'Delhi')
            Selec_columns = ['Day', 'Country_Region', 'Last_Update',
                             'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active',
                             'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio']
            DF_i = DF_day[cond][Selec_columns].reset_index(drop=True)
        except:
            # print(f'{Day} is not available!')
            pass
        return DF_i


    def start(**kwargs):
        import pandas as pd
        Day = '01-01-2021'
        URL_Day = f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{Day}.csv'
        DF_day = pd.read_csv(URL_Day)
        DF_day['Day'] = Day
        cond = (DF_day.Country_Region == 'United Kingdom')
        Selec_columns = ['Day', 'Country_Region', 'Last_Update',
                         'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active',
                         'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio']
        DF_i = DF_day[cond][Selec_columns].reset_index(drop=True)
        DF_i.to_csv('/home/airflow/data/initial.csv')
        LOGGER.info('step 1 done')


    def task_2(**kwargs):
        List_of_days = []
        for year in range(2020, 2022):
            for month in range(1, 13):
                for day in range(1, 32):
                    month = int(month)
                    if day <= 9:
                        day = f'0{day}'

                    if month <= 9:
                        month = f'0{month}'
                    List_of_days.append(f'{month}-{day}-{year}')


    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=start,
        dag=dag
    )
