from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException
from scripts.data_extraction.raw_data_extractor import *
from scripts.data_extraction.spotify_items_staging import *

def create_staging_task_groups(item:str, task_group:str):
    with TaskGroup(f"stage_{item}s") as stage_item:
        @task
        def get_new_items_task():
            return get_new_items(item)

        @task
        def split_into_batches_task(items):
            return split_into_batches(items, batch_size=50)
        
        @task
        def process_batch_task(batch):
            return process_spotify_batch(batch, item_type=item)
        
        @task()
        def retry_items_task(**context):
            batch_results = context["ti"].xcom_pull(task_ids=f"spotify_items_staging.{task_group}.stage_{item}s.process_batch_task")
            failed_items = []
            for batch in batch_results:
                if batch["processed_items"] == 0:
                    failed_items.extend(batch["failed_items"])
            
            return retry_items(failed_items, item_type=item)
        
        @task(trigger_rule="none_failed_min_one_success")
        def item_group_final_log_task(**context):
            batch_results = context["ti"].xcom_pull(task_ids=f"spotify_items_staging.{task_group}.stage_{item}s.process_batch_task")
            retry_results = context["ti"].xcom_pull(task_ids=f"spotify_items_staging.{task_group}.stage_{item}s.retry_items_task") or {}
            return item_group_final_log(extraction_stats=batch_results, retry_stats=retry_results, item_type=item)
        
        @task.branch
        def check_errors_task(batch_results, **context):
            failed_items = []

            for batch in batch_results:
                if batch["processed_items"] == 0:
                    failed_items.extend(batch["failed_items"])
                    
            if failed_items:
                return f"spotify_items_staging.{task_group}.stage_{item}s.retry_items_task"
            else:
                return f"spotify_items_staging.{task_group}.stage_{item}s.item_group_final_log_task"
            
        items = get_new_items_task()
        batches = split_into_batches_task(items)
        batch_results = process_batch_task.expand(batch=batches)
        errors_check = check_errors_task(batch_results)
        retry_output = retry_items_task()
        final_log = item_group_final_log_task()
        
        items >> batches >> batch_results >> errors_check
        errors_check >> [retry_output, final_log]
        retry_output >> final_log

    return stage_item

with DAG("spotify_etl_dag", schedule_interval=None, catchup=False):
    # Tasks to read raw data files and load them into staging
    with TaskGroup("raw_files_extraction") as raw_files_extraction:
        @task
        def get_files_list_task():
            return get_raw_files_list("/opt/airflow/raw_data")
        
        @task
        def get_max_ts_task():
            return get_max_ts()
        
        @task
        def extract_raw_data_task(file_name, max_ts):
            return extract_streaming_history(file_name=file_name, max_ts=max_ts)
        
        @task 
        def extraction_final_log_task(extraction_stats):
            status = extraction_final_log(extraction_stats)
            if status == "Skip downstream":
                raise AirflowSkipException("No new data to process, skipping downstream tasks.")

        files_list = get_files_list_task()
        max_ts = get_max_ts_task()
        extraction_stats = extract_raw_data_task.partial(max_ts=max_ts).expand(file_name=files_list)
        final_log = extraction_final_log_task(extraction_stats)

        [files_list, max_ts] >> extraction_stats >> final_log
    
    with TaskGroup("spotify_items_staging") as spotify_items_staging:
        SPOTIFY_ITEM_TYPES = ["track", "episode", "artist", "podcast"] # ORDER IS IMPORTANT HERE: tracks before artists, episodes before podcasts
        with TaskGroup("stage_tracks_and_episodes") as stage_tracks_and_episodes:
            for item in SPOTIFY_ITEM_TYPES[0:2]: # First stage only tracks and episodes
                create_staging_task_groups(item, task_group="stage_tracks_and_episodes")

        # Dummy task to ensure that if the first group is skipped (all items already staged), the second group still runs
        dummy_success = EmptyOperator(task_id="dummy_success", trigger_rule="none_failed")

        with TaskGroup("stage_artists_and_podcasts") as stage_artists_and_podcasts:
            for item in SPOTIFY_ITEM_TYPES[2:]: # Then stage artists and podcasts
                create_staging_task_groups(item, "stage_artists_and_podcasts")
        
        stage_tracks_and_episodes >> dummy_success >> stage_artists_and_podcasts
                    
    raw_files_extraction >> spotify_items_staging