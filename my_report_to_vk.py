#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'mtalianskaia',
    'depends_on_past': False,
    'retries': 3
}

dag = DAG('mtalianskaia_send_to_vk', 
          default_args=default_args,
          schedule_interval='00 12 * * 1',
          start_date = datetime(2021, 1, 14))

def send_to_vk():
    import pandas as pd
    import numpy as np
    import vk_api
    import random
    
    print ('Libs are loaded')
    
    #Getting data
    data = pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vR-ti6Su94955DZ4Tky8EbwifpgZf_dTjpBdiVH0Ukhsq94jZdqoHuUytZsFZKfwpXEUCKRFteJRc9P/pub?gid=889004448&single=true&output=csv')
    print ('Data is loaded')
    
    #Calculating metrics
    views = data.query("event == 'view'")         .groupby('date')         .event         .count()
    
    clicks = data.query("event == 'click'")         .groupby('date')         .event         .count()
    
    CTR = data.query("event == 'click'").groupby('date').event.count() / data.query("event == 'view'").groupby('date').event.count()*100
    
    money = data.ad_cost.unique()/1000*views
    
    print('Metrics are calculated')
    
    #Calculating differences
    views_diff = round((views[1] - views[0])/views[0]*100,0)
    clicks_diff = round((clicks[1] - clicks[0])/clicks[0]*100,0)
    CTR_diff = round((CTR[1] - CTR[0])/CTR[0]*100,0)
    money_diff = round((money[1] - money[0])/clicks[0]*100,0)
    
    print('Differences are calculated')
    
    #Creating report
    message = f"""Отчет по объявлению 121288 за 2 апреля
                Траты: {money[1]} рублей ({money_diff} %)
                Показы: {views[1]} ({views_diff} %)
                Клики: {clicks[1]} ({clicks_diff} %)
                CTR: {CTR[1]} ({CTR_diff} %)
                """
      
    with open(f'report-2019-04-02.txt', 'w') as f:
    f.write(message)
    
    print('Report is ready')
    
    #Sending report
    app_token = ''
    chat_id = 1234
    my_id = 4321
    vk_session = vk_api.VkApi(token = app_token)
    vk = vk_session.get_api()

    vk.messages.send(
    chat_id = chat_id,
    random_id = random.randint(1,100),
    message = message)
    
    print('Report is sent')


task = PythonOperator(task_id='send_to_vk', 
                    python_callable=send_to_vk(),
                    dag=dag)

