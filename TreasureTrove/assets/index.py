from dagster import (
    asset,
    AssetExecutionContext,
    AssetKey,
    AssetIn,
    StaticPartitionsDefinition, io_manager, resource
)
import pandas as pd
import os
from TreasureTrove.resources import EnvResource
from TreasureTrove.io_managers.pandas_io_manager import column_check, get_path, ingestion_output
from datetime import datetime
import tushare as ts
from utils.tushare import TushareFetcher, get_ts_source_last_trade_date_by_tscode

index_ts_code_mapping = {
    "中证500": "000905.SH",
    "沪深300": "000300.SH",
    "上证指数": "000001.SH",
    "创业板指": "399006.SZ",
    "中证1000": "000852.SH",
    "科创50": "000688.SH",
    "中证2000": "932000.CSI",
}


@asset(
    group_name='Ingestion',
    key=AssetKey(["sources", "tushare", "china_index_daily"]),
)
def china_index_daily(context: AssetExecutionContext, env: EnvResource):
    """
    A股指数日线数据
    :param context:
    :param env:
    :return:
    """
    # tushare初始化
    ts.set_token(env.tushare_token)
    pro = ts.pro_api()

    ts_codes = list(index_ts_code_mapping.values())

    # 读取存储数据，确定每个指数的最后更新日期
    default_trade_date = pd.to_datetime('19910101')  # 默认交易起始日期

    asset_key_path = context.asset_key.path
    dir_path, file_name, file_path = get_path(asset_key_path)
    last_dates = get_ts_source_last_trade_date_by_tscode(
        path=file_path,
        ts_codes=ts_codes,
        default_trade_date=default_trade_date,
        context=context,
    )

    daily_fetcher = TushareFetcher(
        fetch_func=pro.index_daily,
        ts_codes=ts_codes,
        window_days=5000,
        context=context,
        max_rows=7000,
    )

    results = []
    for index, row in last_dates.iterrows():
        batch = daily_fetcher.single_ts_code_fetch(
            ts_code=row['ts_code'],
            start_date=row['trade_date'].strftime('%Y%m%d'),
            end_date=datetime.now().strftime('%Y%m%d'),
        )
        results.append(batch)

    ingestion_output(asset_key_path, context, pd.concat(results))


@asset(
    group_name='China_index',
    key=AssetKey(["staging", "index", "china_index_daily"]),
    io_manager_key="pandas_csv",
)
def stg_china_index_daily(context: AssetExecutionContext, env: EnvResource) -> pd.DataFrame:
    """
    A股指数日线数据标准化清洗
    :param context:
    :param env:
    :return:
    """
    path = os.path.join(env.warehouse_path, 'sources', 'tushare', 'china_index_daily.csv')
    daily = pd.read_csv(path)
    # 字段重命名
    daily.rename(columns={'ts_code': 'uni_code'}, inplace=True)

    # 时间格式化 全部统一改为 2012-2-12 这样的
    daily['trade_date'] = pd.to_datetime(daily['trade_date'], format='%Y%m%d')

    # 单位标准化
    daily['amount'] = round(daily['amount'] * 1000)
    context.log.info(f"A股指数日线数据共\n{len(daily)}条")

    return daily


@asset(
    group_name='Ingestion',
    key=AssetKey(["sources", "tushare", "china_index_info"]),
    io_manager_key="pandas_csv",
)
def china_index_info(context: AssetExecutionContext, env: EnvResource) -> pd.DataFrame:
    """
    A股指数基本信息
    :param context:
    :param env:
    :return:
    """
    # tushare初始化
    ts.set_token(env.tushare_token)
    pro = ts.pro_api()

    markets = ['MSCI', 'CSI', 'SSE', 'SZSE', 'CICC', 'SW', 'OTH']

    results = []
    for market in markets:
        batch = pro.index_basic(market=market)
        context.log.info(f"已获取到{market}市场指数信息，共{len(batch)}条数据")
        results.append(batch)
    return pd.concat(results)
