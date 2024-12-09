import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    # getting spotify credentials
    cilent_id = os.environ.get('CLIENT_ID')
    client_secret = os.environ.get('CLIENT_SECRET')
    
    # Authentication
    client_credentials_manager = SpotifyClientCredentials(client_id=cilent_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

    # playlist link
    playlist_link = "https://api.spotify.com/v1/playlists/4Q8uTgbO8BRVVVqemK9KId"
    playlist_URI = playlist_link.split("/")[-1]
    spotify_data = sp.playlist_tracks(playlist_URI)    
    
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    
    boto3.client('s3').put_object(
    Bucket="spotify-streaming-etl-pipeline",
    Key="raw_data/to_process/" + filename,
    Body=json.dumps(spotify_data)
    )