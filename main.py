import sys
import os
import pandas as pd
from dotenv import load_dotenv
sys.path.append(os.path.realpath(__file__).split("spotify_pipeline")[0]+"spotify_pipeline")

from modules.spotify.spotify_functions import *
from lib.helper import *

#Load in env variables
load_dotenv()
spotify_client_id = os.environ.get("spotify_client_id")
spotify_client_secret = os.environ.get("spotify_client_secret")
spotify_username = os.environ.get("spotify_username")
spotify_redirect_url = os.environ.get("spotify_redirect_url")

# Define which playlists to pull data from
playlist_ids = ["37i9dQZEVXbLRQDuF5jeBp", "2WLEaVPEEX377VUMhpHlDq", "2UDq4hyOlRVhWomJfw3z93"]

# Sets up token and Spotipy object
token = get_token(spotify_client_id, spotify_client_secret, spotify_username, spotify_redirect_url)
sp = initalize_spotipy(token)

# Gets base data for songs/artists
base = get_playlist_tracks(sp=sp, token=token, playlist_id=playlist_ids)
# print(base)

# Gets base for recently played songs
recent = get_recently_played(sp)
# print(recent)

# Gets normalized playlist info for playlists specified
playlist_data = get_playlist_normalized(sp=sp, token=token, playlist_id=playlist_ids)
# print(playlist_data)

# Gets normalized playlist info for recently played songs
recent_playlist_data = pd.DataFrame([["Recent", "Nikhil's Recently Played", "nkatti", '']], columns=["playlist_id", 'playlist_name', 'playlist_owner', 'playlist_url'])
# print(recent_playlist_data)

# Gets normalized songs for playlists specified
song_data = get_songs_normalized(base)
# print(song_data)

# Gets normalized songs for recently played songs
recent_song_data = get_songs_normalized(recent)
# print(recent_song_data)

# Gets normalized artists for playlists specified
artist_data = get_artists_normalized(base)
# print(artist_data)

# Gets normalized artists for recently played songs
recent_artist_data = get_artists_normalized(recent)
# print(recent_artist_data)

# Gets total fact table for playlists specified
fact_table = get_playlist_song_artist(base)
# print(fact_table)

# Gets fact table for recently played songs
fact_recent_songs = get_playlist_song_artist(recent)
# print(fact_recent_songs)

# Combines all dataframes, drop duplicates, and resets indices
all_playlists = combine_dfs(playlist_data, recent_playlist_data)
all_artists = combine_dfs(artist_data, recent_artist_data)
all_songs = combine_dfs(song_data, recent_song_data)

# Gets song features
features = get_song_features(song_df=all_songs, sp=sp)
# print(features)





