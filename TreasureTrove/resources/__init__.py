import os
from typing import Optional

from dagster import ConfigurableResource, Config

WAREHOUSE_PATH = os.getenv("WAREHOUSE_BASE_PATH")


class EnvResource(ConfigurableResource):
    warehouse_path: str
    tushare_token: str
    airtable_api_token: str
    airtable_base_id: str


class NasResource(ConfigurableResource):
    nas_user: str
    nas_password: str
