import pandas as pd
import spotipy.util as util
import sys
import requests
import json
import os
import dotenv
from dotenv import load_dotenv
import spotipy
import copy

# Tables needed: songs, artists, song features, playlists details (popular vs. Nikhil's)

sys.path.append(os.path.realpath(__file__).split("spotify_pipeline")[0]+"spotify_pipeline")

#Load in env variables
load_dotenv()
spotify_client_id = os.environ.get("spotify_client_id")
spotify_client_secret = os.environ.get("spotify_client_secret")
spotify_username = os.environ.get("spotify_username")
spotify_redirect_url = os.environ.get("spotify_redirect_url")

def get_token(id: str, secret: str, user: str, redirect: str, scope: str = "user-read-recently-played playlist-read-private user-top-read playlist-read-collaborative"):
    '''Generates a token based on the env variables that are loaded in and the provided scope'''
    token = util.prompt_for_user_token(user, scope, id, secret, redirect)

    return token


def get_playlist_songs(token: str, playlist_id: list = ["37i9dQZEVXbLRQDuF5jeBp"]):
    '''Returns song information for a given playlist ID. By default, the playlist ID is Spotify's Top 50 - USA playlists
       This playlist is located here: https://open.spotify.com/playlist/37i9dQZEVXbLRQDuF5jeBp?si=76602149e9d4473c  
    '''
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}',
    }

    response = requests.get(f'https://api.spotify.com/v1/playlists/{playlist_id}', headers=headers)
    response = json.loads(response.text)
    return response


def get_playlist_data(token: str, playlist_id: list = ["37i9dQZEVXbLRQDuF5jeBp", "2WLEaVPEEX377VUMhpHlDq", "2UDq4hyOlRVhWomJfw3z93"]):
    sp = spotipy.Spotify(auth=token)

    results = []
    for x in playlist_id:
        result = sp.playlist(x)
        results.append(result)
    return results


def get_playlist_artists(results) -> pd.DataFrame:
    '''Returns artist information for a given playlist ID. By default, the playlist ID is Spotify's Top 50 - USA playlists
       This playlist is located here: https://open.spotify.com/playlist/37i9dQZEVXbLRQDuF5jeBp?si=76602149e9d4473c  
    '''
    all_data = []

    for individual_result in results:
    # Only keep the required items from the results dictionary
        artist_details = individual_result['tracks']['items']

        # Loop through results and add required column to the all_data list
        for x in range(len(artist_details)):
            artists = {}
            song_id = artist_details[x]['track']['id']
            for y in artist_details[x]['track']['artists']:
                artists['artist_id'] = y['id']
                artists['artist_name'] = y['name']
                artists['artist_type'] = y['type']
                artists['artist_url'] = y['external_urls']['spotify']
                artists['song_id'] = song_id
                all_data.append(copy.deepcopy(artists))
        
    # Return dataframe of the all_data list
    return pd.DataFrame(all_data)


def get_playlist_songs(results):
    '''Returns song information for a given playlist ID. By default, the playlist ID is Spotify's Top 50 - USA playlists
       This playlist is located here: https://open.spotify.com/playlist/37i9dQZEVXbLRQDuF5jeBp?si=76602149e9d4473c  
    '''


    all_data = []

    for individual_result in results:
        # Only keep the required items from the results dictionary
        playlist_id = individual_result['id']
        playlist_name = individual_result['name']
        playlist_url = individual_result['external_urls']['spotify']
        song_details = individual_result['tracks']['items']

        # Loop through results and add required column to the all_data list
        
        for x in range(len(song_details)):
            songs = {}

            # Set song-related information
            songs['song_id'] = song_details[x]['track']['id']
            songs['song_name'] = song_details[x]['track']['name']
            songs['song_url'] = song_details[x]['track']['external_urls']['spotify']
            songs['song_popularity'] = song_details[x]['track']['popularity']
            songs['song_release_date'] = song_details[x]['added_at']

            # Convert song duration from milliseconds to decimal form
            millis = int(song_details[x]['track']['duration_ms'])
            seconds = round(float(((millis/1000)%60)/60), 2)
            minutes = int((millis/(1000*60))%60)
            songs['song_duration'] = minutes + seconds
            songs['song_duration_ms'] = song_details[x]['track']['duration_ms']

            # Set playlist-related information
            songs['playlist_id'] = playlist_id
            songs['playlist_name'] = playlist_name
            songs['playlist_url'] = playlist_url

            all_data.append(copy.deepcopy(songs))
    
    # Return dataframe of the all_data list
    return pd.DataFrame(all_data)
                

# def union_dfs()

# Sets up token
token = get_token(spotify_client_id, spotify_client_secret, spotify_username, spotify_redirect_url)

# Gets all playlist info for a playlist
playlist_data = get_playlist_data(token=token)

# for keys in playlist_data[0]:
#     print(keys)

# Gets all artist info for a playlist
# artist_data = get_playlist_artists(playlist_data)
# print(artist_data)

# Gets all song info for a playlist
song_data = get_playlist_songs(playlist_data)
print(song_data)

# print(get_playlist_songs(playlist_data).columns)



