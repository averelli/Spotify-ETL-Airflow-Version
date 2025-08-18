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


def populate_dim_reason(db, logger) -> float:
        """
        Repopulates dim_reason in case there are new reasons added
        Args:
            db: Database connection hook.
            logger: Logger instance.
        Returns:
            int: total time.
        """
        start_time = time.perf_counter()
        logger.info("Started repopulating dim_reason")

        try:
            query = """
            INSERT INTO core.dim_reason (reason_type, reason_group)
            SELECT DISTINCT reason_start AS reason_type, 'start' AS reason_group FROM staging.streaming_history
            UNION ALL
            SELECT DISTINCT reason_end, 'end' AS reason_group FROM staging.streaming_history
            ON CONFLICT DO NOTHING;
            """
            db.execute_query(query)

            total_time = round(time.perf_counter() - start_time, 2)
            logger.info(f"Finished repopulating dim_reason, took {total_time} seconds")
            return total_time

        except Exception as e:
            logger.error(f"Error while repopulating dim_reason: {e}")
            raise


def insert_core_facts(db, logger, item_type:str) -> float:
        """
        Loads new fact records into the core fact table for the specified item type.
        
        For item_type "track" or "podcast", this function executes an INSERT query that
        joins the staging table with the appropriate dimension tables to generate fully transformed fact rows. It uses a delta load
        approach based on the maximum timestamp already present in the fact table.        
        Args:
            db: Database connection hook.
            logger: Logger instance.
            item_type (str): The type of fact to load, either "track" or "podcast".            
        Returns:
            float: The time taken to execute the insertion.
        Raises:
            ValueError: If an invalid item type is provided.
        """
        # metrics
        time_start = time.perf_counter()
        row_count = 0

        logger.info(f"Started inserting data into fact_{item_type}s_history")

        if item_type == "track":
            query = """
            INSERT INTO core.fact_tracks_history (
            ts_msk, date_fk, time_fk, ms_played, sec_played, 
            track_fk, artist_fk, reason_start_fk, reason_end_fk, 
            shuffle, percent_played, offline, offline_timestamp
            )
            SELECT
                s.ts AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow' AS ts_msk,
                d.date_id,
                t.time_id,
                s.ms_played,
                s.ms_played / 1000,
                dt.track_id,
                da.artist_id,
                rs.reason_id,
                re.reason_id,
                s.shuffle,
                round(s.ms_played::numeric / NULLIF(dt.duration_ms, 0) * 100, 1),
                s.offline,
                s.offline_timestamp
            FROM staging.streaming_history s
            LEFT JOIN core.dim_date d ON d.date = (s.ts AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow')::date
            LEFT JOIN core.dim_time t ON t.time = date_trunc('minute', s.ts AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow')::time
            LEFT JOIN core.dim_track dt ON s.spotify_track_uri = dt.spotify_track_uri
            LEFT JOIN core.dim_artist da ON dt.spotify_artist_uri = da.spotify_artist_uri
            LEFT JOIN core.dim_reason rs ON s.reason_start = rs.reason_type AND rs.reason_group = 'start'
            LEFT JOIN core.dim_reason re ON s.reason_end = re.reason_type AND re.reason_group = 'end'
            WHERE 
                s.spotify_track_uri IS NOT NULL 
                AND 
                s.ts > (
                SELECT COALESCE(MAX(ts_msk AT TIME ZONE 'Europe/Moscow' AT TIME ZONE 'UTC'), '1900-01-01'::timestamp)
                FROM core.fact_tracks_history
            );
            """
        elif item_type == "podcast":
            query = """
            INSERT INTO core.fact_podcasts_history (ts_msk, date_fk, time_fk, sec_played, episode_fk, podcast_fk, reason_start_fk, reason_end_fk)
            SELECT
                s.ts AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow' AS ts_msk,
                d.date_id,
                t.time_id,
                s.ms_played / 1000,
                COALESCE(de.episode_id, 0),
                COALESCE(dp.podcast_id, 0),
                rs.reason_id,
                re.reason_id
            FROM staging.streaming_history s
            LEFT JOIN core.dim_date d ON d.date = (s.ts AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow')::date
            LEFT JOIN core.dim_time t ON t.time = date_trunc('minute', s.ts AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow')::time
            LEFT JOIN core.dim_episode de ON s.spotify_episode_uri = de.spotify_episode_uri
            LEFT JOIN core.dim_podcast dp ON de.spotify_podcast_uri = dp.spotify_podcast_uri
            LEFT JOIN core.dim_reason rs ON s.reason_start = rs.reason_type AND rs.reason_group = 'start'
            LEFT JOIN core.dim_reason re ON s.reason_end = re.reason_type AND re.reason_group = 'end'
            WHERE
                s.spotify_episode_uri IS NOT NULL
                AND
                s.ts > (
                    SELECT COALESCE(MAX(ts_msk AT TIME ZONE 'Europe/Moscow' AT TIME ZONE 'UTC'), '1900-01-01'::timestamp)
                    FROM core.fact_podcasts_history
            );
            """
        else:
            logger.error(f"Invalid item type passed. Expected 'track' or 'podcast', got: {item_type}")
            raise ValueError(f"Invalid item type passed. Expected 'track' or 'podcast', got: {item_type}")
        
        try:
            row_count = db.execute_query(query, return_rowcount=True)

            total_time = round(time.perf_counter() - time_start, 2)
            logger.info(f"Inserted {row_count} rows into fact_tracks_history in {total_time} seconds")

            return total_time
        
        except Exception as e:
            logger.error(f"Error while inserting data into fact_{item_type}s_history: {e}")
            raise

def cleanup_staging(db, logger) -> float:
        """
        Cleans up staging layer. Streaming history is truncated, in other tables rows are deleted if 'is_processed' is marked as TRUE.
        
        Args:
            db: Database connection hook.
            logger: Logger instance.
        Returns:
            float: total time to finish the process.
        """
        start_time = time.perf_counter()
        logger.info("Started staging cleaning up process.")

        with db.transaction() as tx_cursor:
            try:
                tx_cursor.execute("TRUNCATE TABLE staging.streaming_history;")
                tx_cursor.execute("DELETE FROM staging.spotify_tracks_data WHERE is_processed = TRUE;")
                tx_cursor.execute("DELETE FROM staging.spotify_episodes_data WHERE is_processed = TRUE;")
                tx_cursor.execute("DELETE FROM staging.spotify_artists_data WHERE is_processed = TRUE;")
                tx_cursor.execute("DELETE FROM staging.spotify_podcasts_data WHERE is_processed = TRUE;")
                
                logger.info("Staging cleanup completed successfully.")
                total_time = round(time.perf_counter() - start_time, 2)

                return total_time
            except Exception as e:
                logger.error(f"Error during staging cleanup: {e}", exc_info=True)
                raise