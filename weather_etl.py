#!/usr/bin/env python
# coding: utf-8

# In[10]:


import requests
import pandas as pd 
from datetime import datetime
import datetime
import boto3
import s3fs

def return_dataframe():
    
     
    today = datetime.datetime.now()

    
    r = requests.get("http://api.weatherapi.com/v1/current.json?key=f41f07114c5c48e682b131944230412&q=Dehradun&aqi=yes")

    data = r.json()
    temp=[]
    wind=[]
    condition=[]
    precip=[]
    humidity=[]
    feelslike=[]
    pressure=[]
    visibility=[]
    is_day=[]
    timestamp=[]

    temp.append(data["current"]["temp_c"])
    wind.append(data["current"]["wind_kph"])
    condition.append(data["current"]["condition"]["text"])
    precip.append(data["current"]["precip_mm"])
    humidity.append(data["current"]["humidity"])
    feelslike.append(data["current"]["feelslike_c"])
    pressure.append(data["current"]["pressure_mb"])
    visibility.append(data["current"]["vis_km"])
    is_day.append(data["current"]["is_day"])
    timestamp.append(today)

    weather_dict = {
    "temperature":temp,
    "wind_speed":wind,
    "condition":condition,
    "precipitation":precip,
    "humidity":humidity,
    "feels_like_temp":feelslike,
    "pressure":pressure,
    "visibility":visibility,
    "is_day":is_day,
    "timestamp":timestamp
    
    }

    weather_df= pd.DataFrame(weather_dict,columns=["temperature","wind_speed","condition","precipitation","humidity","feelslike","pressure","visbility","is_day","timestamp"])
    return weather_df

def data_quality(load_df):
    if load_df.empty:
        print("No weather data")
        return False
    if load_df.isnull().values.any():
        print("Null values found")

def transform_df(load_df):
    load_df["is_day"]=load_df["is_day"].astype(bool)
    load_df["ID"]=load_df['timestamp'].astype(str)+"-"+load_df["temperature"].astype(str)
    return load_df

def weather_etl():
    load_df = return_dataframe()
    data_quality(load_df)
    load_df=transform_df(load_df)
    return load_df
    
def final_etl():
    df=weather_etl()
    s3=boto3.resource('s3')
    aws_access_key='{key}'
    aws_secret='{}'
    df.to_csv(f"s3://urooj-bucket/weather_data.csv",mode='a',index=False,header=False,storage_options={
    "key":aws_access_key,
    "secret":aws_secret})
    
