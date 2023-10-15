from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import argparse


#Criando d função que será manipulada pelo SparkSubmit
#O Orquestrador ficará responsável por criar a sessão spark
def TramentoSilverAlbums(spark, file_path, outputfile): 
    
    #Lendo a pasta de json
    dataset = spark.read.json(file_path)
    dataset.cache()
    
    #Montando o dataset
    df_album = dataset.select(f.explode('tracks.items').alias('items')) \
    .select(
        f.col('items.album.artists')[0]['id'].alias('artist_of_the_publication_id'),
        f.col('items.album.artists')[0]['name'].alias('artist_of_the_publication'),
        f.col('items.album.artists')[0]['type'].alias('type_artist_of_the_publication'),
        f.col('items.album.artists')[0]['href'].alias('href_artist_of_the_publication'),
        f.col('items.id').alias('music_id'),
        f.col('items.name').alias('music_name'),
        f.col('items.popularity').alias('music_popularity'),
        f.col('items.type').alias('music_type'),
        f.col('items.disc_number').alias('disc_number'),
        f.col('items.track_number').alias('track_number'),
        'items.duration_ms',
        'items.explicit',
        'items.external_urls.spotify',
        f.col('items.album.name').alias('music/playlist_name'),
        f.col('items.album.id').alias('music/playlist_id'),
        f.col('items.album.is_playable').alias('music/playlist_is_playable'),
        f.col('items.album.album_type').alias('music/playlist_type'),
        f.col('items.album.release_date').alias('music/playlist_release_date'),
        f.col('items.album.total_tracks').alias('music/playlist_total_tracks'),
        f.explode('items.artists').alias('artist')
    ).select(
        'artist.id',
        'artist.name',
        'artist.type',
        f.col('artist.external_urls.spotify').alias('link_artist'),
        'artist_of_the_publication_id',
        'artist_of_the_publication',
        'type_artist_of_the_publication',
        'href_artist_of_the_publication',
        'music_id',
        'music_name',
        'track_number',
        'music_popularity',
        'music_type',
        'disc_number',
        'duration_ms',
        'explicit', 
        'spotify',
        'music/playlist_id',
        'music/playlist_name',
        'music/playlist_is_playable',
        'music/playlist_type',
        'music/playlist_release_date',
        'music/playlist_total_tracks'
    )
    
    #Salvando o dataset
    df_album.coalesce(1).write.mode('overwrite').json(outputfile)

    
if __name__ == "__main__":
    
    #Exemplo de uso
    
    parser = argparse.ArgumentParser(
        description='Spark Twitter Transformation'
    )
    
    parser.add_argument('--file_path', required=True)
    parser.add_argument('--outputfile', required=True)

    
    args = parser.parse_args()
    
    spark = SparkSession\
            .builder\
                .appName('Spotify_tratramentos')\
                    .getOrCreate()
    
    TramentoSilverAlbums(spark, args.file_path, args.outputfile)

# Exemplo de chamada pelo terminal
#python3.9 /home/gabrielgalani/Documents/Airflow/Scr/TramentoSilverSpotify.py --file_path /home/gabrielgalani/Documents/Airflow/Datalake/Bronze/Spotify  --outputfile /home/gabrielgalani/Documents/Airflow/Scr/teste