from hooks.db_postgres_hook import PgConnectHook
from hooks.spotify_api_hook import SpotifyClientHook
from airflow.utils.log.logging_mixin import LoggingMixin
from spotipy.exceptions import SpotifyException
import time


def get_new_items(entity_type: str):
        """
        Returns a list of only the new unique items from the staged data, 
        excluding those already in the core and previous staging history.

        Args:
            entity_type (str): `track`, `episode`, `artist`, `podcast`
        Returns:
            list: new items to process
        """
        logger = LoggingMixin().log
        logger.info(f"Getting new items for {entity_type}")
        db = PgConnectHook()

        # URIs already staged in history. All items from the raw data. The biggest set
        if entity_type in ["artist", "podcast"]:
            staged_history_items = db.get_staged_uri_from_json(uri_type=entity_type) 
        else:
            staged_history_items = db.get_distinct_uri(uri_type=entity_type, table="staging.streaming_history")

        # URIs already in the core dimension table
        existing_core_items = db.get_distinct_uri(uri_type=entity_type, table=f"core.dim_{entity_type}")

        # URIs from the current staging load. If the previous run crashed but some items were staged, we exclude them
        staged_items = db.get_distinct_uri(uri_type=entity_type, table=f"staging.spotify_{entity_type}s_data")

        # Exclude already processed and previously staged URIs
        new_items = list(set(staged_history_items) - set(existing_core_items) - set(staged_items))
        return new_items


def split_into_batches(items:list, batch_size:int=50) -> list:
    """
    Splits a list of URIs into batches of a specified size

    Args:
        items (list): The list to split
        batch_size (int): The size of each batch

    Returns:
        list: A list of batches, where each batch is a list of items
    """
    logger = LoggingMixin().log
    logger.info(f"Splitting {len(items)} items into batches of size {batch_size}")
    return [items[i:i + batch_size] for i in range(0, len(items), batch_size)] if items else []


def process_spotify_batch(batch: list, item_type:str, retry_limit:int = 2) -> dict:
        """
        Process a single batch of tracks or episodes.
        Args:
            batch (list): a list of Spotify URIs to process
            item_type (str): `track`, `episode`, `artist` or `podcast`
            retry_limit (int): Number of retries allowed before failing. 2 by default
        Returns:
            dict: A dictionary with the processing status and details. Status can contain an error type if the batch fails.
            {"status": str, "processed_items": int, "failed_items": list, "processing_time": float}

        """
        # setup the hooks
        db = PgConnectHook()
        logger = LoggingMixin().log
        sp = SpotifyClientHook()

        # API call function for each type
        api_calls = {
            "track": sp.get_tracks,
            "episode": sp.get_episodes,
            "artist": sp.get_artists,
            "podcast": sp.get_podcasts
        }

        # initialize counters
        retry_counter = 0
        failed_items_counter = 0
        batch_time_start = time.perf_counter()  

        logger.info(f"Started processing a batch of {item_type}s")

        while retry_counter < retry_limit:
            try:
                # call Spotify API to get data
                api_call = api_calls[item_type]
                api_response = api_call(batch)
                # from API we get a dict like: {'tracks': [tracks data]} so to insert into staging each track as individual row we select the list 
                data_key = list(api_response.keys())[0]
                data_items = api_response[data_key]
                items_count = len(data_items)

                # Extract IDs from the response to map URIs
                fetched_data = {item.get("uri"): item for item in data_items if item}  # Filter out None values

                # Identify URIs that returned null and log them into db, no use retrying them since the response is null
                failed_uris = [(uri, item_type, "API returned null") for uri in batch if fetched_data.get(uri) is None]
                failed_items_counter = len(failed_uris)
                if failed_items_counter >= 1:
                    logger.warning(f"{failed_items_counter} failed URIs detected")
                    db.bulk_insert("etl_internal.failed_uris", ["uri", "entity_type", "error_reason"], failed_uris)

                # Convert dict to list for insertion
                valid_data = list(fetched_data.values())

                # insert raw data into staging
                db.bulk_insert(f"staging.spotify_{item_type}s_data", [f"spotify_{item_type}_uri", "raw_data"], valid_data, wrap_json=True)
                
                # track and log the time
                end_time = time.perf_counter() - batch_time_start
                logger.info(f"Processed batch with {items_count} items in {end_time:.2f} seconds on attempt {retry_counter+1}")

                return {
                    "status": "Success",
                    "processed_items": items_count,
                    "failed_items": [],
                    "processing_time": end_time
                }

            except SpotifyException as e:
                # catch a Spotify error: either a rate limit or something wrong with credentials 

                end_time = time.perf_counter() - batch_time_start

                if e.http_status == 429:  # rate limit error
                    wait_time = int(e.headers.get("Retry-After", 60))
                    logger.warning(f"Batch exceeded rate limit. Attempt {retry_counter+1} took {end_time:.2f} seconds. Waiting for {wait_time} seconds.")
                    time.sleep(wait_time)
                    
                    retry_counter += 1

                elif e.http_status == 400: # invalid uri
                    logger.error(f"Batch failed with HTTP 400. Retrying items individually in the next task.")
                    end_time = time.perf_counter() - batch_time_start

                    return {
                        "status": "Invalid URI",
                        "processed_items": 0,
                        "failed_items": batch,
                        "processing_time": end_time,
                    }

                else:
                    # potentially allow a retry in the future
                    logger.error(f"Unexpected Spotify error in batch: {e}", exec_info=True)
                    end_time = time.perf_counter() - batch_time_start

                    return {
                        "status": "Unexpected Spotify error",
                        "processed_items": 0,
                        "failed_items": batch,
                        "processing_time": end_time
                    }

            except Exception as e:
                end_time = time.perf_counter() - batch_time_start
                logger.error(f"Unexpected error in batch after {end_time:.2f} seconds: {e}", exec_info=True)
                
                return {
                    "status": "Unexpected error",
                    "processed_items": 0,
                    "failed_items": batch,
                    "processing_time": end_time
                }
        
        # if retries fail return fail and log failed URIs
        logger.error(f"Exceeded retries for batch")
        end_time = time.perf_counter() - batch_time_start
        return {
            "status": "Failed after retries",
            "processed_items": 0,
            "failed_items": batch,
            "processing_time": end_time
        }


def retry_items(items:list, item_type:str):
    """
    Retries failed items

    Args:
        items (list): List of items to retry.
        item_type (str): Type of the item, can be `track`, `episode`, `artist` or `podcast`.
    Returns:
        tuple[int, int]: A tuple containing the number of valid and invalid items.
    """
    # setup the hooks
    db = PgConnectHook()
    logger = LoggingMixin().log
    sp = SpotifyClientHook()

    start_time = time.perf_counter()

    invalid_uris = []
    valid_data = []

    # API call function for each type
    api_calls = {
        "track": sp.get_tracks,
        "episode": sp.get_episodes,
        "artist": sp.get_artists,
        "podcast": sp.get_podcasts
    }
    api_call = api_calls[item_type]

    for item in items:
        try:
            item_data = api_call(item)
            # append item uri and the data
            valid_data.append((item, item_data))

        except SpotifyException as single_e:
            if single_e.http_status == 429:  # rate limit error
                wait_time = int(single_e.headers.get("Retry-After", 60))
                time.sleep(wait_time)
                items.append(item)  # re-add the item to the list for retry
                
            elif single_e.http_status == 400:
                logger.warning(f"Invalid URI detected: {item}")
                invalid_uris.append((item, item_type, "Invalid URI"))
            else:
                raise  # If another error occurs, let it bubble up
        
    if invalid_uris:
        logger.info(f"Logging {len(invalid_uris)} invalid URIs to etl_internal.failed_uris")
        db.bulk_insert("etl_internal.failed_uris", ["uri", "entity_type", "error_reason"], invalid_uris)

    # Insert valid URIs
    db.bulk_insert(f"staging.spotify_{item_type}s_data", [f"spotify_{item_type}_uri", "raw_data"], valid_data, wrap_json=True)
    
    end_time = time.perf_counter() - start_time

    return {
        "processed_items": len(valid_data),
        "failed_items_count": len(invalid_uris),
        "processing_time": end_time
    }

def item_group_final_log(extraction_stats: list, item_type: str, retry_stats: dict=None):
    """
    Logs the final statistics of the item extraction process.

    Args:
        extraction_stats (list): List of dicts containing the stats for each batch
        retry_stats (dict): Dict containing the stats for the retry_items function
        item_type (str): Type of the item, can be `track`, `episode`, `artist` or `podcast`
    """
    logger = LoggingMixin().log
    total_processed = sum(stat["processed_items"] for stat in extraction_stats) + retry_stats.get("processed_items", 0)
    total_failed = retry_stats.get("failed_items", 0)
    total_time = sum(stat["processing_time"] for stat in extraction_stats) + retry_stats.get("processing_time", 0)

    logger.info(f"Extraction completed for {item_type}s: {total_processed} items processed, {total_failed} items failed, total time: {total_time:.2f} seconds")