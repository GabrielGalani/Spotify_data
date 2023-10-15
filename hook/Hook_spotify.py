from airflow.providers.http.hooks.http import HttpHook
import sys 
sys.path.append(r'/home/gabrielgalani/Documents/Airflow')
from pathlib import Path
import requests
from requests import post
import base64
import json
from dotenv import load_dotenv
import os
import pandas as pd
from pathlib import Path
import time

#Criando hook do spotify
class HookSpotify():
    def __init__(self, year, month, country, conn_id=None):
        
        #Variáveis universais da classe
        self.year = year
        self.month = month
        self.country = country

    #Definindo função que coleta as credenciais do spotify    
    def coleta_credenciais(self): 
        load_dotenv()
        client_id = os.getenv("client_id_spotify")
        client_secret = os.getenv("client_secret_spotify")
        
        return client_id, client_secret
    
    #Coletando o token de acesso à API
    def get_token(self):
    
        credenciais = self.coleta_credenciais()
        credentials = f'{credenciais[0]}:{credenciais[1]}'
        credentials_base64 = base64.b64encode(credentials.encode()).decode()


        url = 'https://accounts.spotify.com/api/token'
        data = {
            'grant_type': 'client_credentials'
        }
        headers = {
            'Authorization': f'Basic {credentials_base64}'
        }

        #Fazendo a solicitação à API
        response = requests.post(url, data=data, headers=headers)
        
        #Coletando o retorno do token ou o erro
        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get('access_token')
            return access_token
        else:
            print(f'Erro na solicitação: {response.status_code}')
            print(response.text)


    #Definindo a função para extrair uma lista com o json com 1000 musicas
    def Search(self, total_songs=1000):
        token = self.get_token()
        headers = {
            'Authorization': "Bearer " + token
        }

        base_url = 'https://api.spotify.com/v1/search'
        query = f'?q=year:{self.year}+month:{self.month}&type=track&market={self.country}&limit=50'

        list_json_response = []
        total_results = 0

        #Fazendo solicitações até alcançar o limite que eu definimos
        while total_results < total_songs:
            response = requests.get(base_url + query, headers=headers)

            if response.status_code == 200:
                data = response.json()
                items = data.get('tracks', {}).get('items', [])

                if not items:
                    break
                
                #Verificando se há duplicados
                unique_items = [item for item in items if item not in list_json_response]
                list_json_response.append({'tracks': {'items': unique_items}})
                total_results += len(unique_items)

                if total_results >= total_songs:
                    break

                query = f'?q=year:{self.year}+month:{self.month}&type=track&market=BR&limit=50&offset={total_results}'

                time.sleep(3)
            else:
                print(f'Erro na solicitação: {response.status_code}')
                print(response.text)
                break
        
        #Retornando json
        return list_json_response


if __name__ == '__main__': 
    
    for pg in HookSpotify('2020-01-01').Search():
        print(json.dumps(pg, indent=4, sort_keys=True))

    