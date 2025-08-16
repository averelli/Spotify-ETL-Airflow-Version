def clean_track(logger, raw_track:dict) -> (tuple[str, str, str, str, str, str, str, str, int, int] | None):
    """
    Transforms a raw track JSON into a clean track tuple.

    Args:
        logger: Logger instance.
        raw_track (dict): Raw JSON from staging.

    Returns:
        tuple: (spotify_track_uri, track_title, cover_art_url, album_name, album_spotify_id, album_type,
                artist_name, artist_uri, release_date, duration_ms, duration_sec)
        or None if an error occurs.
    """
    try:
        # normalize release date based on precision
        precision = raw_track["album"]["release_date_precision"]
        release_date = raw_track["album"]["release_date"]

        clean_date = normalise_date(logger, release_date, precision, raw_track["uri"])

        cover_art_url = raw_track["album"]["images"][0]["url"] if raw_track["album"].get("images") else None

        clean_track = (
            raw_track["uri"], 
            raw_track["name"], 
            cover_art_url, 
            raw_track["album"]["name"], 
            raw_track["album"]["id"], 
            raw_track["album"]["album_type"], 
            raw_track["artists"][0]["name"], 
            raw_track["artists"][0]["uri"],
            clean_date, 
            raw_track["duration_ms"], 
            int(round(raw_track["duration_ms"] / 1000, 0))
        )

        return clean_track

    except Exception as e:
        logger.error(f"Error cleaning track data for track {raw_track.get('uri')}: {e}")
        return None

def clean_artist(logger, raw_artist:dict) -> (tuple[str, str, str] | None):
    """
    Transforms a raw artist JSON into a clean artist tuple.

    Args:
        logger: Logger instance.
        raw_artist (dict): Raw JSON from staging.

    Returns:
        tuple: (spotify_artist_uri, artist_image_url, artist_name) 
        or None if an error occurs.
    """
    try:
        cover_art_url = raw_artist["images"][0]["url"] if raw_artist.get("images") else None
        clean_artist = (
            raw_artist["uri"],
            cover_art_url,
            raw_artist["name"],
        )
        return clean_artist
    except Exception as e:
        logger.error(f"Error cleaning artist data for artist {raw_artist.get('uri')}: {e}")
        return None

def clean_podcast(logger, raw_podcast:dict) -> (tuple[str, str, str, str] | None):
    """
    Transforms a raw podcast JSON into a clean podcast tuple.

    Args:
        logger: Logger instance.
        raw_podcast (dict): Raw podcast JSON from the staging layer.

    Returns:
        tuple or None: (spotify_podcast_uri, podcast_name, description, podcast_cover_art_url),
        or None if an error occurs.
    """
    try:
        cover_art_url = raw_podcast["images"][0]["url"] if raw_podcast.get("images") else None
        clean_podcast = (
            raw_podcast["uri"],
            raw_podcast["name"],
            raw_podcast["description"],
            cover_art_url
        )
        return clean_podcast

    except Exception as e:
        logger.error(f"Error cleaning podcast data for artist {raw_podcast.get('uri')}: {e}")
        return None

def clean_episode(logger, raw_episode:dict) -> (tuple[str, int, int, str, str, str] | None):
    """
    Transforms a raw episode JSON into a clean episode tuple.

    Args:
        logger: Logger instance.
        raw_episode (dict): Raw episode JSON from the staging layer.

    Returns:
        tuple or None: (spotify_episode_uri, duration_ms, duration_sec, podcast_name, spotify_podcast_uri, release_date), 
        or None if an error occurs.
    """
    try:
        release_date = raw_episode["release_date"]
        precision = raw_episode["release_date_precision"]

        clean_date = normalise_date(logger, release_date, precision, raw_episode["uri"])

        clean_episode = (
            raw_episode["uri"],
            raw_episode["duration_ms"],
            int(round(raw_episode["duration_ms"] / 1000, 0)),
            raw_episode["show"]["name"],
            raw_episode["show"]["uri"],
            clean_date
        )
        return clean_episode

    except Exception as e:
        return None

def normalise_date(logger, release_date:str, precision:str, item_uri:str) -> str:
    """
    Normalizes a release date based on its precision.

    If the release date starts with '0000', logs a warning and returns a default date.
    For 'year' precision, appends '-01-01'; for 'month' precision, appends '-01'; otherwise, returns as is.

    Args:
        logger: Logger instance.
        release_date (str): The raw release date.
        precision (str): The precision of the release date (e.g., 'year', 'month', 'day').
        item_uri (str): The URI of the item, used for logging if needed.

    Returns:
        str: The normalized release date.
    """
    # handle the '0000' edge case
    if release_date.startswith("0000"):
        logger.warning(f"Invalid release date for track {item_uri}: {release_date}. Setting as 1900-01-01.")
        clean_date = "1900-01-01"
    else:
        if precision == "year":
            clean_date = release_date + "-01-01"
        elif precision == "month":
            clean_date = release_date + "-01"
        else:
            clean_date = release_date

    return clean_date
