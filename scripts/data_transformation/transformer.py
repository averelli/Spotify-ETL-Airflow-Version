from psycopg2.extras import execute_values
import time
from typing import Callable
from scripts.data_transformation import cleaning_utils

def _get_item_params(item_type:str, logger) -> (tuple[Callable, list, str]):
    """
    Returns the cleaning function, columns to insert, and table name for the specified item type.

    Args:
        item_type (str): Type of item to process (`episodes`, 'tracks', 'artists', 'podcasts').
        logger: Logger instance.

    Returns:
        tuple: (cleaning_function, columns_to_insert, table_name)
    """
    if item_type == "tracks":
        return (cleaning_utils.clean_track, 
                ["spotify_track_uri", "track_title", "cover_art_url", "album_name", "album_spotify_id", "album_type", "artist_name", "spotify_artist_uri", "release_date", "duration_ms", "duration_sec"], 
                "core.dim_track")
    elif item_type == "artists":
        return (cleaning_utils.clean_artist, 
                ["spotify_artist_uri", "cover_art_url", "artist_name"], 
                "core.dim_artist")
    elif item_type == "podcasts":
        return (cleaning_utils.clean_podcast, 
                ["spotify_podcast_uri", "podcast_name", "description", "podcast_cover_art_url"], 
                "core.dim_podcast")
    elif item_type == "episodes":
        return (cleaning_utils.clean_episode, 
                ["spotify_episode_uri", "duration_ms", "duration_sec", "podcast_name", "spotify_podcast_uri", "release_date"], 
                "core.dim_episode")
    else:
        logger.error(f"Invalid item type passed. Expected 'tracks', 'artists', 'episodes' or 'podcasts', got: {item_type}")
        raise ValueError(f"Invalid item type passed. Expected 'tracks', 'artists', 'episodes' or 'podcasts', got: {item_type}")


def get_staged_items(db, logger, item_type:str) -> list:
    """
    Retrieves staged items from the database for the specified item type.

    Args:
        db: Database connection hook.
        logger: Logger instance.
        item_type (str): Type of item to retrieve (e.g., 'track', 'artist', 'podcast').

    Returns:
        list: List of staged items ids.
    """
    
    # query the staging layer for raw data and IDs
    staged_items = db.execute_query(f"SELECT record_id FROM staging.spotify_{item_type}_data WHERE is_processed = FALSE;")
    if not staged_items:
        logger.warning("No unprocessed staged items found")
        return []
    logger.info(f"Retrieved {len(staged_items)} unprocessed staged items from the database")

    # flatten the list
    staged_items = [item[0] for item in staged_items]
    return staged_items


def core_batch(db, logger, item_type:str, batch_ids:list) -> (tuple[float, int]):
    """
    Processes a batch of items and inserts them into the core layer.

    Args:
        db: Database connection hook.
        logger: Logger instance.
        item_type (str): Type of item to process (e.g., 'track', 'artist', 'podcast').
        batch_ids (list): List of item ids to process.

    Returns:
        tuple: (processing_time, processed_count)
    """
    start_time = time.perf_counter()
    total_items_count = 0

    cleaning_function, columns, target_table = _get_item_params(item_type, logger)

    # fetch batch items from db
    logger.info(f"Loading {len(batch_ids)} {item_type} items from the database")
    batch = db.load_staged_batch(item_type, batch_ids)
    
    # batch ids to later mark as processed
    processed_batch_ids = []
    # clean rows to insert
    clean_rows = []

    for record_id, raw_data in batch:
        clean_data = cleaning_function(logger, raw_data)
        # if there was an error while cleaning the json, skip over that record
        if not clean_data:
            continue

        processed_batch_ids.append(record_id)
        clean_rows.append(clean_data)

    # insert the batch inside a transaction
    with db.transaction() as tx_cursor:
        try:
            query = f"INSERT INTO {target_table} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
            execute_values(tx_cursor, query, clean_rows)
            inserted = tx_cursor.rowcount

            # mark processed rows
            tx_cursor.execute(f"UPDATE staging.spotify_{item_type}_data SET is_processed = TRUE WHERE record_id IN %s;", (tuple(processed_batch_ids),))

            logger.info(f"Batch done. Inserted {inserted} rows into {target_table}")
            total_items_count += inserted
        
        except Exception as e:
            logger.error(f"Error while inserting and updating batch: {e}")
            raise # raise so the transaction rolls back

    total_time = round(time.perf_counter() - start_time, 2)
    logger.info(f"All batches processed successfully in {total_time} seconds. Inserted {total_items_count} {item_type}")
    
    return total_time, total_items_count