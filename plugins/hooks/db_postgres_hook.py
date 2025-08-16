from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values, Json
from datetime import datetime, timezone
from contextlib import contextmanager

class PgConnectHook(PostgresHook):
    def __init__(self, postgres_conn_id="dwh-connection", *args, **kwargs):
        super().__init__(postgres_conn_id=postgres_conn_id, *args, **kwargs)

    def _get_cursor(self):
        try:
            conn = self.get_conn()
            cursor = conn.cursor()
            return conn, cursor
        except Exception as e:
            self.log.error(f"Error getting DB connection or cursor: {e}")
            raise

    def execute_query(self, query, params=None, manual_fetch:bool=False, return_rowcount:bool=False):
        """
        Execute a single query.

        Args:
            query (str): SQL qeury to execute
            params (tuple): params to insert into the query, None by default
            manual_fetch (bool): If True, fetch results manually, default False
            return_rowcount (bool): If True, return the row count of the executed query, default False
        Returns:
            list | int | None:
                - If manual_fetch is True or query starts with "select", returns the fetched rows.
                - If return_rowcount is True, returns the number of affected rows.
                - Otherwise, returns None.
        """
        conn, cursor = self._get_cursor()

        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()

            if query.strip().upper().startswith("SELECT") or manual_fetch:
                return cursor.fetchall()
            elif return_rowcount:
                return cursor.rowcount
            else:
                return None
            
        except Exception as e:
            self.log.error(f"Error executing query: {e}")
            conn.rollback()
            return None
        finally:
            cursor.close()
            conn.close()
    
    def bulk_insert(self, table_name, columns, records, wrap_json:bool = False):
        """Insert multiple rows using execute_values.
    
        Args:
            table_name (str): Target table name.
            columns (list): List of columns to insert into.
            records (list): List of record tuples.
            wrap_json (bool): If True, wrap dictionary values with psycopg2.extras.Json.
        """
        conn, cursor = self._get_cursor()

        if not records:
            self.log.warning("No records to insert.")
            return
        
        if wrap_json:
            # also include the item's uri as id
            records = [(item.get("uri"), Json(item),) for item in records]

        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
        try:
            execute_values(cursor, query, records)
            conn.commit()
        except Exception as e:
            self.log.error(f"Error in bulk insert: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    @contextmanager
    def transaction(self):
        conn, cursor = self._get_cursor()
        try:
            cursor = conn.cursor()
            cursor.execute("BEGIN;")
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.log.error(f"Transaction error: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def get_distinct_uri(self, uri_type:str, table:str):
        """
        Fetches distinct URIs from the staged data

        Params:
            uri_type (str): Either 'track', 'artist', 'podcast' or 'episode'
            table (str): The table from which to fetch the URIs
        
        Returns:
            list: A list of distinct URIs
        """
        if uri_type not in ["track", "episode", "artist", "podcast"]:
            raise ValueError(f"Invalid uri_type. Must be: track, episode, artist, or podcast. Instead {uri_type} passed")
        
        query = f"SELECT DISTINCT spotify_{uri_type}_uri FROM {table};"
        response = self.execute_query(query)
        result = [row[0] for row in response if row[0] is not None]
        self.log.info(f"Fetched {len(result)} distinct {uri_type} URIs from {table}")
        
        return result
    
    def get_staged_uri_from_json(self, uri_type:str):
        
        if uri_type == "artist":
            query = "SELECT DISTINCT artists ->> 'uri' FROM staging.spotify_tracks_data t, jsonb_array_elements(raw_data -> 'artists') as artists;"
        elif uri_type == "podcast":
            query = "select distinct raw_data -> 'show' ->> 'uri' from staging.spotify_episodes_data t;"

        else:
            raise ValueError("Wrong value for uri_type, only 'artist' or 'podcast' are valid.")
        
        result = self.execute_query(query)
        self.log.info(f"Fetched {len(result)} distinct {uri_type} URIs from staged items")

        return [row[0] for row in result]
    
    def get_max_history_ts(self):
        """Returns the latest date from the core and staged streaming history"""
        max_ts = self.execute_query(
            """
            SELECT GREATEST(
                (SELECT MAX(ts_msk AT TIME ZONE 'Europe/Moscow' AT TIME ZONE 'UTC') FROM core.fact_tracks_history),
                (SELECT MAX(ts_msk AT TIME ZONE 'Europe/Moscow' AT TIME ZONE 'UTC') FROM core.fact_podcasts_history),
                (SELECT MAX(ts) FROM staging.streaming_history)
            ) AS max_msk_timestamp;
            """
        )[0][0]
        if max_ts == None:
            max_ts = datetime(1900, 1, 1, tzinfo=timezone.utc)

        self.log.info(f"Max timestamp from core and staging: {max_ts}")

        return max_ts
    
    def load_staged_batch(self, item_type:str, batch_ids:list) -> list:
        """
        Loads a batch of items from the database for processing.

        Args:
            item_type (str): Type of item to load (e.g., 'track', 'artist', 'podcast').
            batch_ids (list): List of item ids to load.

        Returns:
            list: List of tuples containing record_id and raw data for each item.
        """
        query = f"SELECT record_id, raw_data FROM staging.spotify_{item_type}_data WHERE record_id IN %s;"
        items = self.execute_query(query, (tuple(batch_ids),))
        
        if not items:
            self.log.warning(f"No items found for {item_type} with ids {batch_ids}")
            return []
        
        self.log.info(f"Loaded {len(items)} items for {item_type} from the database")
        return items
