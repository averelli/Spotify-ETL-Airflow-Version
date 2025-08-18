from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.log.logging_mixin import LoggingMixin
from scripts.data_extraction import raw_data_extractor as extractor
from scripts.data_extraction import spotify_items_staging as staging
from scripts.data_transformation import transformer
from scripts.utils import split_into_batches
from hooks.db_postgres_hook import PgConnectHook
from hooks.spotify_api_hook import SpotifyClientHook

def create_staging_task_groups(item:str, task_group:str):
    with TaskGroup(f"stage_{item}s") as stage_item:
        @task
        def get_new_items_task():
            logger = LoggingMixin().log
            db = PgConnectHook()
            return staging.get_new_items(db, logger, item)

        @task
        def split_into_staging_batches_task(items):
            logger = LoggingMixin().log
            return split_into_batches(logger, items, batch_size=50)
        
        @task
        def process_batch_task(batch):
            logger = LoggingMixin().log
            db = PgConnectHook()
            sp_hook = SpotifyClientHook()
            return staging.process_spotify_batch(db, logger, sp_hook, batch, item_type=item)
        
        @task()
        def retry_items_task(**context):
            batch_results = context["ti"].xcom_pull(task_ids=f"spotify_items_staging.{task_group}.stage_{item}s.process_batch_task")
            failed_items = []
            for batch in batch_results:
                if batch["processed_items"] == 0:
                    failed_items.extend(batch["failed_items"])

            logger = LoggingMixin().log
            db = PgConnectHook()
            sp_hook = SpotifyClientHook()
            
            return staging.retry_items(db, logger, sp_hook, failed_items, item_type=item)
        
        @task(trigger_rule="none_failed_min_one_success")
        def item_group_final_log_task(**context):
            batch_results = context["ti"].xcom_pull(task_ids=f"spotify_items_staging.{task_group}.stage_{item}s.process_batch_task")
            retry_results = context["ti"].xcom_pull(task_ids=f"spotify_items_staging.{task_group}.stage_{item}s.retry_items_task") or {}
            logger = LoggingMixin().log
            return staging.item_group_final_log(logger, extraction_stats=batch_results, retry_stats=retry_results, item_type=item)
        
        @task.branch
        def check_errors_task(batch_results):
            failed_items = []

            for batch in batch_results:
                if batch["processed_items"] == 0:
                    failed_items.extend(batch["failed_items"])
                    
            if failed_items:
                return f"spotify_items_staging.{task_group}.stage_{item}s.retry_items_task"
            else:
                return f"spotify_items_staging.{task_group}.stage_{item}s.item_group_final_log_task"
            
        items = get_new_items_task()
        batches = split_into_staging_batches_task(items)
        batch_results = process_batch_task.expand(batch=batches)
        errors_check = check_errors_task(batch_results)
        retry_output = retry_items_task()
        final_log = item_group_final_log_task()
        
        items >> batches >> batch_results >> errors_check
        errors_check >> [retry_output, final_log]
        retry_output >> final_log

    return stage_item

def load_dims_task_groups(item_type:str):
    with TaskGroup(f"load_{item_type}_dim") as load_dim:
        @task
        def get_staged_items_task():
            db = PgConnectHook()
            logger = LoggingMixin().log
            staged_items = transformer.get_staged_items(db, logger, item_type)
            return staged_items

        @task 
        def split_into_core_batches_task(staged_items):
            logger = LoggingMixin().log
            return split_into_batches(logger, staged_items, batch_size=50)
        
        @task 
        def core_batch_task(batch):
            db = PgConnectHook()
            logger = LoggingMixin().log
            batch_time, batch_count = transformer.core_batch(db, logger, item_type, batch)
            return batch_time, batch_count
        
        staged_items = get_staged_items_task()
        batches = split_into_core_batches_task(staged_items)
        batch_results = core_batch_task.expand(batch=batches)

        staged_items >> batches >> batch_results

    return load_dim


with DAG("spotify_etl_dag", schedule_interval=None, catchup=False):
    # Tasks to read raw data files and load them into staging
    with TaskGroup("raw_files_extraction") as raw_files_extraction:
        @task
        def get_files_list_task():
            return extractor.get_raw_files_list("/opt/airflow/raw_data")
        
        @task
        def get_max_ts_task():
            db = PgConnectHook()
            return extractor.get_max_ts(db)
        
        @task
        def extract_raw_data_task(file_name, max_ts):
            db = PgConnectHook()
            logger = LoggingMixin().log
            return extractor.extract_streaming_history(db, logger, file_name=file_name, max_ts=max_ts)
        
        @task
        def extraction_final_log_task(extraction_stats):
            logger = LoggingMixin().log
            return extractor.extraction_final_log(logger, extraction_stats)
            
        files_list = get_files_list_task()
        max_ts = get_max_ts_task()
        extraction_stats = extract_raw_data_task.partial(max_ts=max_ts).expand(file_name=files_list)
        final_log = extraction_final_log_task(extraction_stats)

        [files_list, max_ts] >> extraction_stats >> final_log

    @task.branch
    def skip_staging_tasks(final_log):
        raw_extraction_status = final_log["message"]
        if raw_extraction_status != "Skip downstream":
            return "run_spotify_items_staging"
        else:
            return "run_transformation"

    skip_staging_check = skip_staging_tasks(final_log)
    run_spotify_items_staging = EmptyOperator(task_id="run_spotify_items_staging")
    run_transformation = EmptyOperator(task_id="run_transformation", trigger_rule="none_failed")
    
    # Stage Spotify items in batches
    with TaskGroup("spotify_items_staging") as spotify_items_staging:

        SPOTIFY_ITEM_TYPES = ["track", "episode", "artist", "podcast"] # ORDER IS IMPORTANT HERE: tracks before artists, episodes before podcasts

        with TaskGroup("stage_tracks_and_episodes") as stage_tracks_and_episodes:
            for item in SPOTIFY_ITEM_TYPES[0:2]: # First stage only tracks and episodes
                create_staging_task_groups(item, task_group="stage_tracks_and_episodes")

        # Dummy task to ensure that if the first group is skipped (all items already staged), the second group still runs
        @task.branch(trigger_rule="none_failed")
        def run_second_group_task(**context):
            raw_extraction_status = context["ti"].xcom_pull(task_ids="raw_files_extraction.extraction_final_log_task")
            if raw_extraction_status != "Skip downstream":
                return "spotify_items_staging.dummy_success"
            else:
                raise AirflowSkipException("Skipping artists and podcasts staging as raw extraction status is 'Skip downstream tasks'")
        run_second_group_check = run_second_group_task()
        run_stage_artists_and_podcasts = EmptyOperator(task_id="dummy_success")

        with TaskGroup("stage_artists_and_podcasts") as stage_artists_and_podcasts:
            for item in SPOTIFY_ITEM_TYPES[2:]: # Then stage artists and podcasts
                create_staging_task_groups(item, "stage_artists_and_podcasts")

        @task
        def log_staging_stats(**context):
            logger = LoggingMixin().log
            ti = context["ti"]
            file_extraction_stats = ti.xcom_pull(task_ids="raw_files_extraction.extraction_final_log_task")
            track_stats = ti.xcom_pull(task_ids="spotify_items_staging.stage_tracks_and_episodes.stage_tracks.item_group_final_log_task")
            episode_stats = ti.xcom_pull(task_ids="spotify_items_staging.stage_tracks_and_episodes.stage_episodes.item_group_final_log_task")
            artist_stats = ti.xcom_pull(task_ids="spotify_items_staging.stage_artists_and_podcasts.stage_artists.item_group_final_log_task")
            podcast_stats = ti.xcom_pull(task_ids="spotify_items_staging.stage_artists_and_podcasts.stage_podcasts.item_group_final_log_task")
            total_time = file_extraction_stats["total_time"] + track_stats["total_time"] + episode_stats["total_time"] + artist_stats["total_time"] + podcast_stats["total_time"]
            logger.info(f"Staging completed in {total_time} seconds: Raw files: {file_extraction_stats["total_time"]} Tracks: {track_stats}, Episodes: {episode_stats}, Artists: {artist_stats}, Podcasts: {podcast_stats}")
            return {
                "raw_extraction_time": file_extraction_stats["total_time"],
                "track_stats": track_stats,
                "episode_stats": episode_stats,
                "artist_stats": artist_stats,
                "podcast_stats": podcast_stats,
                "total_time": total_time
            }
        staging_stats= log_staging_stats()    
        
        stage_tracks_and_episodes >> run_second_group_check >> run_stage_artists_and_podcasts >> stage_artists_and_podcasts >> staging_stats

    # Data transformation tasks
    with TaskGroup("data_transformation") as data_transformation:
        DIMENTSION_ITEM_TYPES = ["tracks", "artists", "podcasts", "episodes"]
        FACT_ITEM_TYPES = ["track", "podcast"]
        
        # Populate core dims
        with TaskGroup("load_spotify_dims") as load_spotify_dims:
            for item_type in DIMENTSION_ITEM_TYPES:
                load_dim = load_dims_task_groups(item_type)

        @task
        def log_spoti_dims_load(**context):
            logger = LoggingMixin().log
            ti = context["ti"]
            dim_stats = {item_type: {
                                    "total_time": 0,
                                    "processed_count": 0
                                } for item_type in DIMENTSION_ITEM_TYPES}
            
            for item_type in DIMENTSION_ITEM_TYPES:
                batch_results = ti.xcom_pull(task_ids=f"data_transformation.load_spotify_dims.load_{item_type}_dim.core_batch_task")
                if batch_results:
                    total_time = sum(batch[0] for batch in batch_results)
                    processed_count = sum(batch[1] for batch in batch_results)
                    dim_stats[item_type]["total_time"] = total_time
                    dim_stats[item_type]["processed_count"] = processed_count

            total_time = sum(stat.get("total_time", 0) for stat in dim_stats.values())
            total_processed_count = sum(stat.get("processed_count", 0) for stat in dim_stats.values())

            dim_stats["total_time"] = total_time
            dim_stats["total_processed_count"] = total_processed_count

            logger.info(f"Spotify dims loaded: {dim_stats}")
            return dim_stats
        spoti_dim_stats = log_spoti_dims_load()

        # Populate dim_reason table
        @task(trigger_rule="none_failed")
        def populate_dim_reason_task():
            db = PgConnectHook()
            logger = LoggingMixin().log
            return transformer.populate_dim_reason(db, logger)
        run_populate_dim_reason = populate_dim_reason_task()
        
        # Insert facts into core tables
        with TaskGroup("load_facts") as load_spotify_facts:
            @task
            def insert_core_facts_task(item_type):
                db = PgConnectHook()
                logger = LoggingMixin().log
                return transformer.insert_core_facts(db, logger, item_type)
            insert_track_facts = insert_core_facts_task.override(task_id="insert_track_facts")("track")
            insert_podcast_facts = insert_core_facts_task.override(task_id="insert_podcast_facts")("podcast")

            @task
            def log_facts_load(**context):
                logger = LoggingMixin().log
                ti = context["ti"]
                track_time = ti.xcom_pull(task_ids="data_transformation.load_facts.insert_track_facts")
                podcast_time = ti.xcom_pull(task_ids="data_transformation.load_facts.insert_podcast_facts")
                logger.info(f"Track facts loaded in {track_time} seconds, Podcast facts loaded in {podcast_time} seconds, total time: {track_time + podcast_time} seconds")
                return {
                    "track_time": track_time,
                    "podcast_time": podcast_time,
                    "total_time": track_time + podcast_time
                }
            run_log_facts_load = log_facts_load()

            insert_track_facts >> run_log_facts_load
            insert_podcast_facts >> run_log_facts_load

        # Cleanup staging layer
        @task
        def cleanup_staging_task():
            db = PgConnectHook()
            logger = LoggingMixin().log
            return transformer.cleanup_staging(db, logger)
        run_cleanup_staging = cleanup_staging_task()

        @task
        def final_transformation_log_task(**context):
            logger = LoggingMixin().log
            ti = context["ti"]
            dim_stats = ti.xcom_pull(task_ids="data_transformation.log_spoti_dims_load")
            dim_reason_time = ti.xcom_pull(task_ids="data_transformation.populate_dim_reason_task")
            facts_stats = ti.xcom_pull(task_ids="data_transformation.load_facts.log_facts_load")
            cleanup_time = ti.xcom_pull(task_ids="data_transformation.cleanup_staging_task")

            total_time = dim_stats["total_time"] + dim_reason_time + facts_stats["total_time"] + cleanup_time
            logger.info(f"Data transformation completed in {total_time} seconds. Dims: {dim_stats}, Dim Reason Time: {dim_reason_time}, Facts: {facts_stats}, Cleanup Time: {cleanup_time}")

            return total_time
        final_transformation_log = final_transformation_log_task()

        load_spotify_dims >> spoti_dim_stats >> run_populate_dim_reason >> load_spotify_facts >> run_cleanup_staging >> final_transformation_log

    # final etl log
    @task
    def final_etl_log_task(**context):
        logger = LoggingMixin().log
        ti = context["ti"]
        staging_time = ti.xcom_pull(task_ids="spotify_items_staging.log_staging_stats")["total_time"] or 0
        transformation_time = ti.xcom_pull(task_ids="data_transformation.final_transformation_log_task")
        
        total_time = staging_time + transformation_time
        logger.info(f"ETL process completed in {total_time} seconds. Extraction: {staging_time} seconds, Transformation: {transformation_time} seconds")
    final_etl_log = final_etl_log_task()

    raw_files_extraction >> skip_staging_check >> [run_spotify_items_staging, run_transformation] 
    run_spotify_items_staging >> spotify_items_staging >> run_transformation
    run_transformation >> data_transformation >> final_etl_log