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


def get_playlist_api(token: str, playlist_id = "2WLEaVPEEX377VUMhpHlDq"):
    '''Returns song information for a given playlist ID. By default, the playlist ID is Spotify's Top 50 - USA playlists
       This playlist is located here: https://open.spotify.com/playlist/37i9dQZEVXbLRQDuF5jeBp?si=76602149e9d4473c  
    '''
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}',
    }

    sp = spotipy.Spotify(auth=token)
    response = requests.get(f'https://api.spotify.com/v1/playlists/{playlist_id}?limit=1000', headers=headers)
    response = json.loads(response.text)
    return response


def get_playlist_tracks(token: str, playlist_id: list = ["37i9dQZEVXbLRQDuF5jeBp", "2WLEaVPEEX377VUMhpHlDq", "2UDq4hyOlRVhWomJfw3z93"]) -> list:
    sp = spotipy.Spotify(auth=token)

    results = []
    for x in playlist_id:
        result = sp.playlist_items(x, offset=0, limit=100)
        tracks = result['items']

        while result['next']:

            result = sp.next(result)
            tracks.extend(result['items'])
        
        # Append the playlist ID onto the track so that we can later use this
        for track_no in range(len(tracks)):
            tracks[track_no]['playlist_id'] = x

        results.append(tracks)
    
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


def get_playlist_song_artist(results) -> pd.DataFrame:
    '''Returns song information for a given playlist ID. By default, the playlist ID is Spotify's Top 50 - USA playlists
       This playlist is located here: https://open.spotify.com/playlist/37i9dQZEVXbLRQDuF5jeBp?si=76602149e9d4473c  
    '''

    all_data = []

    for individual_result in results:
        song_details = individual_result

        # Loop through results and add required column to the all_data list   
        for x in range(len(song_details)):
            for y in song_details[x]['track']['artists']:
                songs = {}
                songs['artist_id'] = y['id']
                # Set song-related information
                songs['song_id'] = song_details[x]['track']['id']
                songs['playlist_id'] = song_details[x]['playlist_id']
                all_data.append(copy.deepcopy(songs))
    
    # Return dataframe of the all_data list
    df = pd.DataFrame(all_data)
    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

    
def get_playlist_normalized(token: str, playlist_id: list = ["37i9dQZEVXbLRQDuF5jeBp", "2WLEaVPEEX377VUMhpHlDq", "2UDq4hyOlRVhWomJfw3z93"]):
    '''Takes in original playlist data and outputs a unique list of playlists that will be used for playlist dimension table'''
    sp = spotipy.Spotify(auth=token)

    results = []
    for x in playlist_id:
        result = sp.playlist(x)
        results.append(result)

    all_data = []

    for individual_result in results:
        playlist = {}
        playlist['playlist_id'] = individual_result['id']
        playlist['playlist_name'] = individual_result['name']
        playlist['playlist_owner'] = individual_result['owner']['display_name']
        playlist['playlist_url'] = individual_result['external_urls']['spotify']
        all_data.append(copy.deepcopy(playlist))

    df = pd.DataFrame(all_data)
    df.drop_duplicates(subset=['playlist_id'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

def get_songs_normalized(results) -> pd.DataFrame:
    '''Takes in original playlist data and outputs a unique list of songs that will be used for playlist dimension table'''
    all_data = []

    for individual_result in results:
        song_details = individual_result

        # Loop through results and add required column to the all_data list
        
        for x in range(len(song_details)):
            songs = {}

            # Set song-related information
            songs['song_id'] = song_details[x]['track']['id']
            songs['song_name'] = song_details[x]['track']['name']
            songs['song_popularity'] = song_details[x]['track']['popularity']
            songs['song_release_date'] = song_details[x]['added_at']

            try:
                songs['song_url'] = song_details[x]['track']['external_urls']['spotify']   
            except:
                songs['song_url'] = ''

            # Convert song duration from milliseconds to decimal form
            millis = int(song_details[x]['track']['duration_ms'])
            seconds = round(float(((millis/1000)%60)/60), 2)
            minutes = int((millis/(1000*60))%60)
            songs['song_duration'] = minutes + seconds
            songs['song_duration_ms'] = song_details[x]['track']['duration_ms']

            all_data.append(copy.deepcopy(songs))
    
    # Removes duplicates and returns dataframe of all de-duped songs
    df = pd.DataFrame(all_data)
    df.drop_duplicates(subset=['song_id'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

# Can have one song show up twice in the same playlist - need to drop duplicates
# Each song can have multiple artists so it will have to repeat -- need to make an additional loop on the number of items in artist
                
def get_artists_normalized(results) -> pd.DataFrame:
    '''Takes in original playlist data and outputs a unique list of artists that will be used for playlist dimension table'''
    all_data = []

    for individual_result in results:
        # Only keep the required items from the results dictionary
        artist_details = individual_result

        # Loop through results and add required column to the all_data list
        for x in range(len(artist_details)):
            artists = {}

            for y in artist_details[x]['track']['artists']:
                artists['artist_id'] = y['id']
                artists['artist_name'] = y['name']
                artists['artist_type'] = y['type']
                try:
                    artists['artist_url'] = y['external_urls']['spotify']
                except:
                    artists['artist_url'] = ''
                all_data.append(copy.deepcopy(artists))
        
    # Removes duplicates and returns dataframe of all de-duped songs
    df = pd.DataFrame(all_data)
    df.drop_duplicates(subset=['artist_id'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

# def union_dfs()

# Sets up token
token = get_token(spotify_client_id, spotify_client_secret, spotify_username, spotify_redirect_url)

# Gets base data for songs/artists
# base = get_playlist_tracks(token=token)
# print(base)

# Gets normalized playlist info
playlist_data = get_playlist_normalized(token=token)
print(playlist_data)

# # Gets normalized songs
# song_data = get_songs_normalized(base)
# print(song_data)

# # Gets normalized artists
# artist_data = get_artists_normalized(base)
# print(artist_data)

# Gets total fact table
# fact_table = get_playlist_song_artist(base)
# print(fact_table)
