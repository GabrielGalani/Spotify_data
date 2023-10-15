from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import sys
sys.path.append(r'/home/gabrielgalani/Documents/Airflow')
from Operators.Spotify_operators import SpotifyOperator 
from os.path import join
from pathlib import Path
import time
import datetime

#Montando a dag do airflow
with DAG (dag_id = "SportifyDAG", 
          start_date=days_ago(2), 
          schedule_interval="@daily")as dag:
    
    #Definindo variáveis 
    BASEFOLDER = join(
        str(
            Path('~/Documents').expanduser()), 
        'Airflow/Datalake/{stage}/spotify/{partition}',
        )
    PARTITION = f"extract_date={time.strftime('%y-%m-%d')}"

    data_atual = datetime.datetime.now()
    year = str(data_atual.year)
    month = str(data_atual.month)

    #Musicas Brasileiras
    country_br= 'BR'
    file_name = f"Artists&Albums_year={year}_month={month}_country={country_br}_extractDate={time.strftime('%y-%m-%d')}.json"
    
    spotify_operator_br = SpotifyOperator(file_path=join(BASEFOLDER.format(stage='Bronze',partition=PARTITION), file_name), 
                                       year=year, 
                                       month=month, 
                                       country = country_br, 
                                       task_id='spotify_operators_br'
    )
    
    #Musicas americanas
    country_us= 'US'
    file_name = f"Artists&Albums_year={year}_month={month}_country={country_us}_extractDate={time.strftime('%y-%m-%d')}.json"
    
    spotify_operator_us = SpotifyOperator(file_path=join(BASEFOLDER.format(stage='Bronze',partition=PARTITION), file_name), 
                                       year=year, 
                                       month=month, 
                                       country = country_us, 
                                       task_id='spotify_operators_us'
    )
    
    #Musicas britanicas
    country_gb= 'GB'
    file_name = f"Artists&Albums_year={year}_month={month}_country={country_gb}_extractDate={time.strftime('%y-%m-%d')}.json"
    
    spotify_operator_bg = SpotifyOperator(file_path=join(BASEFOLDER.format(stage='Bronze',partition=PARTITION), file_name), 
                                       year=year, 
                                       month=month, 
                                       country = country_gb, 
                                       task_id='spotify_operators_bg'
    )
    
    #Tratamento da camada Silver
    silver_tratament = SparkSubmitOperator(task_id = 'Silver_tratament_spotify',
                                           application= '/home/gabrielgalani/Documents/Airflow/Scr/Scripts/Spark/TramentoSilverSpotify.py',
                                           name = 'silver_tratament_spotify',
                                           application_args=[
                                               '--file_path', BASEFOLDER.format(stage='Bronze', partition=''),
                                               '--outputfile', BASEFOLDER.format(stage='Silver', partition= "{{ ds }}")
                                           ]                    
    )
    
#Definindo a ordem em que as ações são executadas
spotify_operator_br >>  spotify_operator_us >> spotify_operator_bg >> silver_tratament