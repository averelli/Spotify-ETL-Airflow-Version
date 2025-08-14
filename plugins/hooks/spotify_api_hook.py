from airflow.hooks.base import BaseHook
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

class SpotifyClientHook(BaseHook):
    def __init__(self, conn_id="spotify-api"):
        super().__init__()
        self.conn_id = conn_id

    def _get_client(self):
        try:
            conn = self.get_connection(self.conn_id)
            client_credentials_manager = SpotifyClientCredentials(
                client_id=conn.login,
                client_secret=conn.password
            )
            sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
            self.log.info("Spotify client initialized.")
            return sp
        except Exception as e:
            self.log.error(f"Error initializing Spotify client: {e}")
            raise

    def get_tracks(self, tracks: list) -> dict:
        """
        Fetches tracks data from Spotify.

        Args:
            tracks (list): A list of track URIs or IDs (max 50).

        Returns:
            dict: JSON response containing track data.
        """
        sp = self._get_client()
        try:
            response = sp.tracks(tracks)
            return response
        except Exception as e:
            self.log.error(f"Error fetching tracks: {e}")
            raise

    def get_artists(self, artists: list) -> dict:
        """
        Fetches artists data from Spotify.

        Args:
            artists (list): A list of artist URIs or IDs (max 50).

        Returns:
            dict: JSON response containing artists data.
        """
        sp = self._get_client()
        try:
            response = sp.artists(artists)
            return response
        except Exception as e:
            self.log.error(f"Error fetching artists: {e}")
            raise

    def get_podcasts(self, podcasts: list) -> dict:
        """
        Fetches podcasts data from Spotify.

        Args:
            podcasts (list): A list of podcast URIs or IDs (max 50).

        Returns:
            dict: JSON response containing podcasts data.
        """
        sp = self._get_client()
        try:
            response = sp.shows(podcasts)
            return response
        except Exception as e:
            self.log.error(f"Error fetching podcasts: {e}")
            raise
    
    def get_episodes(self, episodes: list) -> dict:
        """
        Fetches episodes data from Spotify.

        Args:
            episodes (list): A list of episode URIs or IDs (max 50).

        Returns:
            dict: JSON response containing episodes data.
        """
        sp = self._get_client()
        try:
            response = sp.episodes(episodes)
            return response
        except Exception as e:
            self.log.error(f"Error fetching episodes: {e}")
            raise
