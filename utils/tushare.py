import pandas as pd
import tushare as ts
from datetime import datetime, timedelta
from typing import Tuple, Callable, Generator
import time
import os


class TushareFetcher:
    def __init__(self,
                 fetch_func: Callable[..., pd.DataFrame],
                 ts_codes: list[str] = None,
                 max_rows: int = 8000,
                 window_days: int = 90,
                 code_field: str = 'ts_code',
                 params: dict = None,
                 context=None
                 ):
        """
        初始化数据获取器。

        :param fetch_func: 获取数据的函数，接受ts_code, start_date, end_date参数
        :param ts_codes: 交易代码，支持输入一串
        :param max_rows: 每批次最大获取行数
        :param window_days: 每次查询的日期范围
        """
        self.fetch_func = fetch_func
        self.ts_codes = ts_codes
        self.ts_code_cnt = len(ts_codes)
        self.max_rows = max_rows
        self.window_days = window_days
        self.context = context
        self.code_field = code_field
        self.params = params

    def _date_range(self,
                    start: datetime,
                    end: datetime
                    ) -> Generator[Tuple[datetime, datetime], None, None]:
        """
        生成日期范围。

        :param start: 开始日期
        :param end: 结束日期
        :return: 生成的日期窗口(start_date, end_date)
        """
        # 计算每个窗口内最多允许的天数
        allowed_days = self.max_rows
        if allowed_days < 1:
            raise ValueError("max_rows太小，无法容纳每个ts_code一天的数据。")

        # 实际查询窗口天数不能超过初始化时设置的window_days
        window_days = min(self.window_days, allowed_days)
        span = timedelta(days=window_days)

        current = start
        while current < end:
            current_end = min(current + span, end)
            yield current, current_end
            current = current_end + timedelta(days=1)

    def single_ts_code_fetch(self,
                             ts_code: str,
                             start_date: str,
                             end_date: str
                             ) -> pd.DataFrame:
        """
        按ts_code, 分批获取数据。适用于ts_code不多，回溯时间长的场景
        :param ts_code: 交易代码
        :param start_date: 查询的起始日期
        :param end_date: 查询的终止日期
        :return: 获取的数据
        """
        start = datetime.strptime(start_date, '%Y%m%d')
        end = datetime.strptime(end_date, '%Y%m%d')
        self.context.log.info(f"windows \nstart_date: {start}, \nend_date: {end}")
        results = []
        for s, e in self._date_range(start, end):
            self.context.log.info(f"Fetching {ts_code} data from {s} to {e}")
            # 构造动态参数字典，TODO：后面要改掉这个丑陋的东西
            if self.params:
                params = self.params.copy()
                params['ts_code'] = ts_code
                params['start_date'] = s.strftime("%Y%m%d")
                params['end_date'] = e.strftime("%Y%m%d")
            else:
                params = {
                    self.code_field: ts_code,
                    "start_date": s.strftime("%Y%m%d"),
                    "end_date": e.strftime("%Y%m%d")
                }
            batch = self.fetch_func(**params)
            time.sleep(3)
            results.append(batch)
            self.context.log.info(f"Fetched {len(batch)} rows")

        if len(results) == 0:
            self.context.log.info(f"No data found for {ts_code}")
            return pd.DataFrame()
        else:
            return pd.concat(results, ignore_index=True)


def get_ts_source_last_trade_date_by_tscode(
        path: str,
        ts_codes: list[str] = None,
        default_trade_date: pd.Timestamp = None,
        context=None
) -> pd.DataFrame:
    """
    获取tscode对应的最后交易日期
    :param path: 存储路径
    :param ts_codes:
    :param default_trade_date: 默认其实日期
    :param context: dg上下文
    :return:
    """
    if not os.path.exists(path):
        context.log.info(f"{path} 文件不存在，返回默认初始交易日")
        trade_date_df = pd.DataFrame({
            'ts_code': ts_codes,
            'trade_date': default_trade_date
        })
    else:
        context.log.info(f"{path} 文件存在，读取文件")
        df = pd.read_csv(path)
        if df.empty:
            context.log.info(f"{path} 文件为空，返回默认初始交易日")
            trade_date_df = pd.DataFrame({
                'ts_code': ts_codes,
                'trade_date': default_trade_date
            })
            return trade_date_df

        # 如果需要，可以将trade_date转换为日期格式：
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')

        # 按ts_code分组，取每组中最大的trade_date
        df_max = df.groupby('ts_code')['trade_date'].max().reset_index()

        # 只保留ts_codes中存在的记录
        df_filtered = df_max[df_max['ts_code'].isin(ts_codes)]

        # 构造包含全部ts_codes的DataFrame，并与查询结果左连接
        trade_date_df = pd.DataFrame({'ts_code': ts_codes}).merge(df_filtered, on='ts_code', how='left')

        # 对于未查到记录的ts_code，填充默认trade_date
        trade_date_df['trade_date'] = trade_date_df['trade_date'].fillna(default_trade_date)

        # 如果trade_date等于今天，则去掉这一行，复制再去掉
        trade_date_df['date'] = trade_date_df['trade_date']
        trade_date_df['date'] = pd.to_datetime(trade_date_df['date']).dt.date
        trade_date_df = trade_date_df[trade_date_df['date'] != pd.Timestamp.now().date()]
        trade_date_df = trade_date_df.drop(columns=['date'], inplace=True)
        # context.log.debug(f"now\n{pd.Timestamp.now().date()}")
    context.log.info(f"目标symbol最后交易日期\n{trade_date_df}")
    return trade_date_df
