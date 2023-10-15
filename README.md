## Projeto de Análise de Músicas (2022-2023)

Este projeto visa disponibilizar um dataset contendo informações sobre músicas lançadas nos anos de 2022-2023, permitindo que seja realizado estudos de análise e ciência de dados. O dataset pode ser encontrado no GitHub e também no Kaggle.

## Documentação da API
A documentação da API utilizada para obter informações sobre as músicas pode ser encontrada em Spotify Web API Documentation.

## Arquitetura do Projeto
Este projeto foi arquitetado com o uso do Apache Airflow. O Airflow é uma plataforma de gerenciamento de fluxo de trabalho que permite agendar e monitorar tarefas de processamento de dados. Ele é ideal para automatizar tarefas de ETL (Extração, Transformação e Carregamento) e é amplamente utilizado em projetos de análise de dados.

## Tecnologias Utilizadas
O projeto utiliza as seguintes tecnologias e ferramentas:

Python: A linguagem de programação principal para desenvolver o projeto.
PySpark: Uma biblioteca Python para processamento de dados distribuídos, que é amplamente utilizada para análise de grandes conjuntos de dados.
Apache Spark: Uma plataforma de processamento de dados distribuídos que é utilizada em conjunto com o PySpark.
Apache Airflow: A plataforma de orquestração de fluxo de trabalho usada para automatizar tarefas de processamento de dados.

## Como Utilizar o Projeto:
1. Clone o repositório do GitHub em sua máquina local.
 ```bash
 git clone https://github.com/GabrielGalani/Spotify_data.git
```
2. Instale as dependências do projeto.
```bash
pip install -r requirements.txt
```
Execute o projeto com o Apache Airflow para coletar e processar os dados das músicas.

```bash
airflow webserver -p 8080  # Inicie o servidor da web do Airflow em http://localhost:8080/
airflow scheduler  # Inicie o agendador do Airflow
```
Acesse o painel do Airflow em http://localhost:8080/ para monitorar e gerenciar o fluxo de trabalho do projeto.

## Conjunto de Dados
O conjunto de dados das músicas está disponível nos seguintes locais:

[1. Kaggle - Dataset](https://www.kaggle.com/datasets/gabrielgalani/spotify-lancamentos-2022-2023)
[2. Github - Dataset](https://github.com/GabrielGalani/Spotify_data/tree/main/Datalake)

## Contribuições
Contribuições são bem-vindas! Sinta-se à vontade para abrir problemas (issues) e enviar pull requests para melhorar este projeto.
