from dagster import (
    Definitions,
    load_assets_from_package_module,
    EnvVar
)
from TreasureTrove.io_managers.pandas_io_manager import CsvIOManager
from TreasureTrove.resources import EnvResource, NasResource
from TreasureTrove.jobs.ingestion_job import ingestion_daily, ingestion_daily_schedule
from TreasureTrove import assets

defs = Definitions(
    assets=load_assets_from_package_module(assets),
    jobs=[ingestion_daily],
    schedules=[ingestion_daily_schedule],
    resources={
        "pandas_csv": CsvIOManager(),
        "env": EnvResource(
            warehouse_path=EnvVar("WAREHOUSE_BASE_PATH"),
            tushare_token=EnvVar("TUSHARE_TOKEN"),
            airtable_api_token=EnvVar("AIRTABLE_API_TOKEN"),
            airtable_base_id=EnvVar("AIRTABLE_BASE_ID"),
        ),
        "nas_env": NasResource(
            nas_user=EnvVar("NAS_USER"),
            nas_password=EnvVar("NAS_PASSWORD"),
        ),
    },
)
