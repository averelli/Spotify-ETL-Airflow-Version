from hooks.db_postgres_hook import PgConnectHook
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timezone
import json
import glob
import time

def get_raw_files_list(raw_data_path: str) -> list:
    """
    Returns a list of raw data files in the specified directory.
    """
    return glob.glob(raw_data_path + "/*.json")


def get_max_ts() -> datetime:
    """
    Retrieves the maximum timestamp from the database.
    """
    db = PgConnectHook()
    return db.get_max_history_ts()


def extract_streaming_history(file_name:str, max_ts:datetime):
    start_time = time.time()

    db = PgConnectHook()
    logger = LoggingMixin().log

    logger.info(f"Started processing file: {file_name}")
    try:
        with open(file_name, "r", encoding="utf-8") as f:
            data = json.load(f)

            columns = ["ts", "platform", "ms_played", "conn_country", "ip_addr", "master_metadata_track_name", "master_metadata_album_artist_name", "master_metadata_album_album_name", "spotify_track_uri", "episode_name", "episode_show_name", "spotify_episode_uri", "reason_start", "reason_end", "shuffle", "skipped", "offline", "offline_timestamp", "incognito_mode"]

            # create records to insert only if the timestamp is later than the max recorded one
            records = [
                (
                    row["ts"],
                    row["platform"],
                    row["ms_played"],
                    row["conn_country"],
                    row["ip_addr"],
                    row["master_metadata_track_name"],
                    row["master_metadata_album_artist_name"],
                    row["master_metadata_album_album_name"],
                    row["spotify_track_uri"],
                    row["episode_name"],
                    row["episode_show_name"],
                    row["spotify_episode_uri"],
                    row["reason_start"],
                    row["reason_end"],
                    row["shuffle"],
                    row["skipped"],
                    row["offline"],
                    row["offline_timestamp"],
                    row["incognito_mode"]
                ) for row in data if datetime.strptime(row["ts"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc) > max_ts
            ]

            # empty file check
            if len(records) == 0:
                logger.info(f"Empty file or nothing to insert: {file_name}")
            else:
                db.bulk_insert("staging.streaming_history", columns, records)

        total_time = time.time() - start_time
    
        # Log success
        record_count = len(records)
        logger.info(f"Successfully processed {file_name}: {record_count} records in {total_time:.2f} seconds")

        return {
            "records_count": record_count,
            "processing_time": total_time
        }

    except json.JSONDecodeError as e:
        logger.error(f"JSON error in {file_name}: {e}")
    except IOError as e:
        logger.error(f"Could not read {file_name}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error processing {file_name}: {str(e)}", exc_info=True)


def extraction_final_log(extraction_stats: list):
    logger = LoggingMixin().log
    total_files = len(extraction_stats)
    total_records = sum(stat["records_count"] for stat in extraction_stats)
    total_time = sum(stat["processing_time"] for stat in extraction_stats)

    if total_files == 0:
        logger.warning("No files processed during extraction")
        return "Skip downstream"
    elif total_records == 0:
        return "Skip downstream"
    else:
        logger.info(f"Extraction complete. Processed {total_files} files, {total_records} total records in {total_time:.2f} seconds.")
        return "Continue downstream"