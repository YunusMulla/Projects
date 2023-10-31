#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from airflow import DAG
from datetime import timedelta,datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
import pandas as pd
import requests


# In[ ]:


#Sample data just to convert json data in panda dataframe
city_name='portland'
base_url='data/2.5/forecast?id=524901&appid'

with open('credential.txt' ,'r') as f: #api key in creditial file 
    api_key=f.read()
    
full_url=base_url+city_name+ "&APIID" + api_key

r=requests.get(full_url)
data=r.json()

print(data)


# In[ ]:


def kelvin_to_fahrenheit(value):
    temp_in_fahrenheit=(value-273.15)*(9/5) * 32
    return temp_in_fahrenheit



def transform_load_data(task_instanse):
    data=task_insanse.xcom_pull(task_id='extract_weather_data')
    city=data['name']
    weather_discription=data['weather'][0]['discription']
    temp_in_fahrenheit=kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_fahrenheit=kelvin_to_fahrenheit(data["main"]["feels"])
    min_temp_fahrenheit=kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_fahrenheit=kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure=data["main"]["pressure"]
    humidity=data["main"]["humidity"]
    wind_speed=data["main"]["speed"]
    time_of_record=datetime.utcfromtimestamp(data['dt']+ data['timezone'])
    sunrise_time=datetime.utcfromtimestamp(data['sys']+ data['sunrise']+ data['timezone'])
    sunset_time=datetime.utcfromtimestamp(data['sys']+ data['sunset'] + data['timezone'])
    
    
    transformed_data={
        'City'=city,
        'discription'=weather_discription,
        'Temp(F)'=temp_in_fahrenheit,
        'Feels_like_temp(F)'=feels_like_fahrenheit,
        'Min_temp(F)'=min_temp_fahrenheit,
        'Max_temp(F)'=max_temp_fahrenheit,
        'Pressure'=pressure,
        'Humidity'=humidity,
        'Wind_speed'=wind_speed,
        'Time_of_record'=time_of_record,
        'Sunrise_time'=sunrise_time,
        'Sunset_time'=sunset_time
        
    }
    
transformed_data_list=[transformed_data]
df=pd.DataFrame(transformed_data_list)

df.to_csv('s3://own-s3/my_folder/-yml(dt_string).csv',index=False)


# In[ ]:


default_args={
    'owner':'Yunus',
    'start_date':datetime(2023 ,12,10),
    'email':['mullayunus036@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':True,
    'retries': 5,
    'retry_delay':timedelta(minutes=2)
}


# In[ ]:


with DAG('weather_dag',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False)as dag:
    
            is_weather_api_ready=HttpSensor(
            task_id='weather_api_ready',
            http_conn_id='weathermap_api',
            end_point='data/2.5/forecast?id=524901&appid=5324JYP89566244524'
        )
        
            extract_weather_data=SimpleHttpOperator(
            task_id='extract_weather_data',
            http_conn_id='weathermap_api',
            endpoint='data/2.5/forecast?id=524901&appid=5324JYP89566244524',
            method='GET',
            response_filter=lambda r:json.loads(r.text),
            Log_response=True
             
        )
            
            transform_load_weather_data=PythonOperator(
            task_id='transform_load_weather_data',
            python_callable=transfer_loada_data 
        )
            
            weather_api_ready >> extract_weather_data >> transform_load_weather_data

