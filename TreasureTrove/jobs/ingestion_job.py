from dagster import define_asset_job, AssetSelection, AssetKey, multiprocess_executor, ScheduleDefinition


ingestion_daily = define_asset_job(
    name="ingestion_daily",
    selection=AssetSelection.groups("Ingestion")
    - AssetSelection.assets(
        AssetKey(["sources", "tushare", "china_index_info"]),
        AssetKey(["sources", "tushare", "china_index_weight"]),
        AssetKey(["sources", "ad_hoc", "bars"]),
    ),
    executor_def=multiprocess_executor.configured({"max_concurrent": 1}),
)


# Define the schedule
ingestion_daily_schedule = ScheduleDefinition(
    job=ingestion_daily,
    cron_schedule="30 16 * * 1-5",  # Every weekday at 16:30
    execution_timezone="Asia/Shanghai"  # Beijing time
)
