from dagster import (
    asset,
    AssetExecutionContext,
    AssetKey,
    AssetIn,
    StaticPartitionsDefinition, io_manager, resource
)
import pandas as pd
import os

from sqlalchemy.sql.functions import current_timestamp

from TreasureTrove.resources import EnvResource, TushareBarsConfig
from TreasureTrove.io_managers.pandas_io_manager import column_check, get_path, ingestion_output
from datetime import datetime
import tushare as ts
from utils.tushare import TushareFetcher, get_ts_source_last_trade_date_by_tscode


@asset(
    group_name='Ingestion',
    key=AssetKey(["sources", "ad_hoc", "bars"]),
)
def bars(context: AssetExecutionContext, env: EnvResource, config: TushareBarsConfig):
    """
    通用行情的日线数据
    :param context:
    :param env:
    :param config: 参数配置
            ops:
              sources__ad_hoc__bars:
                config:
                  params:
                    ts_code: ['601899.SH','600547.SH']
                    start_date: '20000101'
                    end_date: '20250210'
                    asset: 'E'
                    adj: 'hfq'
                    freq: 'D'
                    adjfactor: True
                    https://tushare.pro/document/2?doc_id=109
    :return:
    """
    # tushare初始化
    ts.set_token(env.tushare_token)
    pro = ts.pro_api()

    asset_key_path = context.asset_key.path
    dir_path, file_name, file_path = get_path(asset_key_path)

    ts_codes = config.params['ts_code']
    config.params.pop('ts_code')

    daily_fetcher = TushareFetcher(
        fetch_func=ts.pro_bar,
        ts_codes=ts_codes,
        window_days=1800,
        context=context,
        max_rows=1800,
        params=config.params,
    )

    results = []
    for code in ts_codes:
        context.log.info(f"code: {code}")
        batch = daily_fetcher.single_ts_code_fetch(
            ts_code=code,
            start_date=config.params['start_date'],
            end_date=config.params['end_date'],
        )
        results.append(batch)

    output = pd.concat(results)
    # 按照ts_code和trade_date进行去重
    output.drop_duplicates(subset=['ts_code', 'trade_date'], inplace=True)

    now_timestamp = int(datetime.now().timestamp())
    output.to_csv(os.path.join(dir_path, f'bars_{now_timestamp}.csv'), index=False)

