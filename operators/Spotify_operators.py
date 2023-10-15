from airflow.models import BaseOperator, DAG, TaskInstance
import sys
sys.path.append(r'/home/gabrielgalani/Documents/Airflow')
from hook.Hook_spotify import HookSpotify
import os 
from pathlib import Path 
import json
import time
from os.path import join

#Criando a classe com BaseOperator
class SpotifyOperator(BaseOperator):
    
    template_fields = ["file_path", "year", "month", "country"]
    
    def __init__(self, file_path, year, month, country, **kwargs): 
          
        #Variáveis universais da classe      
        self.file_path = file_path
        self.year = year
        self.month = month
        self.country = country
        super().__init__(**kwargs)
    
    #Função de orquestração de pastas
    def create_parent_folder(self, file_path): 
        (Path(file_path).parent).mkdir(parents=True, exist_ok=True)
      
    #Método que será lido pelo Airflow para montagem dos json Bronze  
    def execute(self, context):
        
        self.create_parent_folder(self.file_path)
        with open(self.file_path, "w") as output:
            for pg in HookSpotify(self.year, self.month, self.country).Search():
                json.dump(pg, output, ensure_ascii=False)
                output.write('\n')
                

if __name__ == '__main__': 
    
    #Teste de rodagem. Extraindo 2 anos inteiros de musicas
    months = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
    countries = ['BR', 'US', 'GB']
    dates = ['2023', '2022']
    BASEFOLDER = join(
        str(
            Path('~/Documents').expanduser()), 
        'Airflow/Datalake/{stage}/spotify/{partition}',
        )
    PARTITION = f"extract_date={time.strftime('%y-%m-%d')}"
    
    for y in dates:
        for m in months:
            for c in countries: 
                print(f'extraindo o ano - {y}, mes - {m}, pais - {c}')
                file_name = f"Artists&Albums_year={y}_month={m}_country={c}_extractDate={time.strftime('%y-%m-%d')}.json"                
                to = SpotifyOperator(file_path=join(BASEFOLDER.format(stage='Bronze',partition=PARTITION), file_name), 
                                     year=y, 
                                     month=m, 
                                     country = c, 
                                     task_id='spotify_operators')
                
                ti = TaskInstance(task=to)
                to.execute(ti.task_id)

        
    
    