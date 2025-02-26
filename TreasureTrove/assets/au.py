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


@asset(
    group_name='Ingestion',
    key=AssetKey(["sources", "tushare", "au", "xau_usd_daily"]),
)
def xau_usd_daily(context: AssetExecutionContext, env: EnvResource):
    """
    国际金价的日线数据
    :param context:
    :param env:
    :return:
    """
    # tushare初始化
    ts.set_token(env.tushare_token)
    pro = ts.pro_api()

    asset_key_path = context.asset_key.path
    dir_path, file_name, file_path = get_path(asset_key_path)
    last_dates = get_ts_source_last_trade_date_by_tscode(
        path=file_path,
        ts_codes=['XAUUSD.FXCM'],
        default_trade_date=pd.to_datetime('19800101'),
        context=context,
    )

    # 外汇美元黄金
    xau_usd_daily_fetcher = TushareFetcher(
        fetch_func=pro.fx_daily,
        ts_codes=['XAUUSD.FXCM'],
        window_days=1000,
        context=context,
        max_rows=1000,
    )

    xau_usd = xau_usd_daily_fetcher.single_ts_code_fetch(
        ts_code='XAUUSD.FXCM',
        start_date=last_dates.iloc[0]['trade_date'].strftime('%Y%m%d'),
        end_date=datetime.now().strftime('%Y%m%d'),
    )

    ingestion_output(asset_key_path, context, xau_usd)


@asset(
    group_name='Ingestion',
    key=AssetKey(["sources", "tushare", "au", "seg_au99_daily"]),
)
def seg_au99_daily(context: AssetExecutionContext, env: EnvResource):
    """
    上海交易所黄金现货的日线数据
    :param context:
    :param env:
    :return:
    """
    # tushare初始化
    ts.set_token(env.tushare_token)
    pro = ts.pro_api()

    asset_key_path = context.asset_key.path
    dir_path, file_name, file_path = get_path(asset_key_path)
    last_dates = get_ts_source_last_trade_date_by_tscode(
        path=file_path,
        ts_codes=['Au99.99'],
        default_trade_date=pd.to_datetime('20000101'),
        context=context,
    )

    daily_fetcher = TushareFetcher(
        fetch_func=pro.sge_daily,
        ts_codes=['Au99.99'],
        window_days=2000,
        context=context,
        max_rows=2000,
    )

    au99 = daily_fetcher.single_ts_code_fetch(
        ts_code='Au99.99',
        start_date=last_dates.iloc[0]['trade_date'].strftime('%Y%m%d'),
        end_date=datetime.now().strftime('%Y%m%d'),
    )

    ingestion_output(asset_key_path, context, au99)


@asset(
    group_name='Ingestion',
    key=AssetKey(["sources", "tushare", "au", "au_etf_daily"]),
)
def au_etf_daily(context: AssetExecutionContext, env: EnvResource):
    """
    上海交易所黄金现货的日线数据
    :param context:
    :param env:
    :return:
    """
    # tushare初始化
    ts.set_token(env.tushare_token)
    pro = ts.pro_api()

    ts_codes = ['518880.SH', '159937.SZ', '159934.SZ']

    asset_key_path = context.asset_key.path
    dir_path, file_name, file_path = get_path(asset_key_path)
    last_dates = get_ts_source_last_trade_date_by_tscode(
        path=file_path,
        ts_codes=ts_codes,
        default_trade_date=pd.to_datetime('20100101'),
        context=context,
    )

    daily_fetcher = TushareFetcher(
        fetch_func=pro.fund_daily,
        ts_codes=ts_codes,
        window_days=2000,
        context=context,
        max_rows=2000,
    )

    results = []
    for index, row in last_dates.iterrows():
        batch = daily_fetcher.single_ts_code_fetch(
            ts_code=row['ts_code'],
            start_date=row['trade_date'].strftime('%Y%m%d'),
            end_date=datetime.now().strftime('%Y%m%d'),
        )
        results.append(batch)

    try:
        output = pd.concat(results)
    except Exception as e:
        context.log.warning(e)
        output = pd.DataFrame()

    ingestion_output(asset_key_path, context, output)


@asset(
    group_name='Au',
    key=AssetKey(["staging", "au", "au_etf_daily"]),
    io_manager_key="pandas_csv",
)
def stg_au_etf_daily(context: AssetExecutionContext, env: EnvResource) -> pd.DataFrame:
    """
    黄金ETF日线数据标准化清洗
    :param context:
    :param env:
    :return:
    """
    path = os.path.join(env.warehouse_path, 'sources', 'tushare', 'au', 'au_etf_daily.csv')
    daily = pd.read_csv(path)
    # 字段重命名
    daily.rename(columns={'ts_code': 'uni_code'}, inplace=True)

    # 时间格式化 全部统一改为 2012-2-12 这样的
    daily['trade_date'] = pd.to_datetime(daily['trade_date'], format='%Y%m%d')

    daily.sort_values(by=['trade_date', 'uni_code'], ascending=[True, True], inplace=True)

    daily['pct_chg'] = round(daily['close'] / daily['pre_close'] - 1, 4)

    # 单位标准化
    daily['amount'] = round(daily['amount'] * 1000)

    context.log.info(f"黄金ETF日线数据共\n{len(daily)}条")

    return daily


@asset(
    group_name='Au',
    key=AssetKey(["staging", "au", "xau_usd_daily"]),
    io_manager_key="pandas_csv",
)
def xau_usd_daily(context: AssetExecutionContext, env: EnvResource) -> pd.DataFrame:
    """
    美元黄金日线数据标准化清洗
    :param context:
    :param env:
    :return:
    """
    path = os.path.join(env.warehouse_path, 'sources', 'tushare', 'au', 'xau_usd_daily.csv')
    daily = pd.read_csv(path)
    # 字段重命名
    daily.rename(columns={
        'ts_code': 'uni_code',
        'bid_close': 'close',
    }, inplace=True)

    # 时间格式化 全部统一改为 2012-2-12 这样的
    daily['trade_date'] = pd.to_datetime(daily['trade_date'], format='%Y%m%d')

    daily.sort_values(by=['trade_date', 'uni_code'], ascending=[True, True], inplace=True)

    # 计算 pre_close
    daily['pre_close'] = daily.groupby('uni_code')['close'].shift(1)
    # 删除无前日数据
    daily.dropna(subset=['pre_close'], inplace=True)

    daily['pct_chg'] = round(daily['close'] / daily['pre_close'] - 1, 4)

    daily = daily[['trade_date', 'uni_code', 'close', 'pre_close', 'pct_chg']]

    context.log.info(f"黄金ETF日线数据共\n{len(daily)}条")

    return daily


@asset(
    group_name='Au',
    key=AssetKey(["staging", "au", "seg_au99_daily"]),
    io_manager_key="pandas_csv",
)
def seg_au99_daily(context: AssetExecutionContext, env: EnvResource) -> pd.DataFrame:
    """
    黄金现货日线数据标准化清洗
    :param context:
    :param env:
    :return:
    """
    path = os.path.join(env.warehouse_path, 'sources', 'tushare', 'au', 'seg_au99_daily.csv')
    daily = pd.read_csv(path)
    # 字段重命名
    daily.rename(columns={'ts_code': 'uni_code'}, inplace=True)

    # 时间格式化 全部统一改为 2012-2-12 这样的
    daily['trade_date'] = pd.to_datetime(daily['trade_date'], format='%Y%m%d')

    daily.sort_values(by=['trade_date', 'uni_code'], ascending=[True, True], inplace=True)

    # 计算 pre_close
    daily['pre_close'] = daily.groupby('uni_code')['close'].shift(1)
    # 删除无前日数据
    daily.dropna(subset=['pre_close'], inplace=True)

    daily['pct_chg'] = round(daily['close'] / daily['pre_close'] - 1, 4)

    context.log.info(f"黄金现货日线数据共\n{len(daily)}条")

    return daily
