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


def _get_ts_source_last_trade_date_by_curve_term(
        path: str,
        curve_terms: list[float] = None,
        default_trade_date: pd.Timestamp = None,
        context=None
) -> pd.DataFrame:
    """
    获取对应期限国债的最后交易日期
    :param path: 存储路径
    :param curve_terms: 国债期限 10年期=10.0
    :param default_trade_date: 默认其实日期
    :param context: dg上下文
    :return:
    """
    if not os.path.exists(path):
        context.log.info(f"{path} 文件不存在，返回默认初始交易日")
        trade_date_df = pd.DataFrame({
            'curve_term': curve_terms,
            'trade_date': default_trade_date
        })
    else:
        context.log.info(f"{path} 文件存在，读取文件")
        df = pd.read_csv(path)
        if df.empty:
            context.log.info(f"{path} 文件为空，返回默认初始交易日")
            trade_date_df = pd.DataFrame({
                'curve_term': curve_terms,
                'trade_date': default_trade_date
            })
            return trade_date_df

        # 如果需要，可以将trade_date转换为日期格式：
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')

        # 按ts_code分组，取每组中最大的trade_date
        df_max = df.groupby('curve_term')['trade_date'].max().reset_index()

        # 只保留ts_codes中存在的记录
        df_filtered = df_max[df_max['curve_term'].isin(curve_terms)]

        # 构造包含全部ts_codes的DataFrame，并与查询结果左连接
        trade_date_df = pd.DataFrame({'curve_term': curve_terms}).merge(df_filtered, on='curve_term', how='left')

        # 对于未查到记录的ts_code，填充默认trade_date
        trade_date_df['trade_date'] = trade_date_df['trade_date'].fillna(default_trade_date)
    context.log.info(f"目标symbol最后交易日期\n{trade_date_df}")
    return trade_date_df


@asset(
    group_name='Ingestion',
    key=AssetKey(["sources", "tushare", "china_bond_ytm_daily"]),
)
def china_bond_ytm_daily(context: AssetExecutionContext, env: EnvResource):
    """
    国债收益率每日数据
    :param context:
    :param env:
    :return:
    """
    # tushare初始化
    ts.set_token(env.tushare_token)
    pro = ts.pro_api()

    terms = [1.0, 5.0, 10.0, 20.0, 30.0, 50.0]

    asset_key_path = context.asset_key.path
    dir_path, file_name, file_path = get_path(asset_key_path)
    last_dates = _get_ts_source_last_trade_date_by_curve_term(
        path=file_path,
        curve_terms=terms,
        default_trade_date=pd.to_datetime('20160601'),
        context=context,
    )

    daily_fetcher = TushareFetcher(
        fetch_func=pro.yc_cb,
        window_days=1500,
        context=context,
        max_rows=2000,
    )

    results = []

    for term in terms:
        batch = daily_fetcher.param_fetch(
            param=term,
            param_name='curve_term',
            start_date=last_dates.iloc[0]['trade_date'].strftime('%Y%m%d'),
            end_date=datetime.now().strftime('%Y%m%d'),
        )
        batch = batch[batch['curve_type'] == '0']
        results.append(batch)

    output = pd.concat(results)

    ingestion_output(asset_key_path, context, output)


@asset(
    group_name='Bond',
    key=AssetKey(["staging", "bond", "china_bond_ytm_daily"]),
    io_manager_key="pandas_csv",
)
def stg_china_bond_ytm_daily(context: AssetExecutionContext, env: EnvResource) -> pd.DataFrame:
    """
    中国国债每日收益率标准化清洗
    :param context:
    :param env:
    :return:
    """
    path = os.path.join(env.warehouse_path, 'sources', 'tushare', 'china_bond_ytm_daily.csv')
    daily = pd.read_csv(path)
    # 字段重命名
    daily.rename(columns={'yield': 'yield_rate'}, inplace=True)

    # 将年期改为int类型
    daily['curve_term'] = daily['curve_term'].astype(int).astype(str)
    daily['symbol'] = 'CN' + daily['curve_term'] + 'Y'
    daily['curve_type'] = '到期'

    daily.drop_duplicates(subset=['trade_date', 'symbol'], inplace=True)

    # 时间格式化 全部统一改为 2012-2-12 这样的
    daily['trade_date'] = pd.to_datetime(daily['trade_date'], format='%Y%m%d')

    daily.sort_values(by=['trade_date', 'symbol'], ascending=[True, True], inplace=True)

    daily.drop(columns=['ts_code', 'curve_term'], inplace=True)

    context.log.info(f"国债收益曲线数据共\n{len(daily)}条")

    return daily
