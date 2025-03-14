import pandas as pd


def calculate_shriller_pe(df: pd.DataFrame, cpi_estimated: float) -> pd.DataFrame:
    """
    计算 shriller pe 指标
    参数:
      df: 包含以下列的DataFrame：
         - trade_date: 日期列
         - dps: 当期每股分红
         - eps: 当期每股收益
         - cpi: 消费者物价指数（单位为百分比）
         - long_interest_rate_gs10: 10年期长期国债收益率（单位为百分比）
         - close: 当日收盘价
      cpi_estimated: 当期cpi预测值
    返回:
      在原有的df中增加以下列：
         - real_total_return_price: 考虑价格和分红的累计回报价格（每日计算，但分红调整按月）
         - shriller_pe: 根据调整后的real_price与120个月滚动平均的real_eps计算，保留两位小数
         - excess_cape_yield: 超额市盈率收益
    """
    df = df.copy().sort_values(by='trade_date')

    # 计算调整后的价格和分红（每日计算，月内dps、eps、cpi、long_interest_rate_gs10均相同）
    df['real_price'] = df['close'] * cpi_estimated / df['cpi']
    df['real_dividend'] = df['dps'] * cpi_estimated / df['cpi']

    # 新增月份列，用于识别每个月
    df['month'] = df['trade_date'].dt.to_period('M')

    # 计算每日比例因子：
    # 同一月内，比例因子为当前real_price与前一日real_price之比；
    # 跨月时（即当前行与前一行月份不同），比例因子为：
    # (当前real_price + 当前real_dividend/12) / 前一日real_price
    ratios = [1]
    for i in range(1, len(df)):
        if df['month'].iloc[i] == df['month'].iloc[i-1]:
            ratio = df['real_price'].iloc[i] / df['real_price'].iloc[i-1]
        else:
            ratio = (df['real_price'].iloc[i] + df['real_dividend'].iloc[i] / 12) / df['real_price'].iloc[i-1]
        ratios.append(ratio)
    df['ratio'] = ratios

    # 计算累计总回报价格
    df['real_total_return_price'] = df['real_price'].iloc[0] * df['ratio'].cumprod()
    df.drop(columns=['ratio'], inplace=True)

    # 计算调整后的每股收益
    df['real_eps'] = df['eps'] * cpi_estimated / df['cpi']

    # 按月提取real_eps（假设每月内数据一致，取每月第一天的值）
    monthly_eps = df.groupby('month')['real_eps'].first()
    # 计算120个月滚动均值（先移位一个月，确保当前月不参与计算）
    monthly_rolling_avg_eps = monthly_eps.shift(1).rolling(window=120, min_periods=120).mean()
    # 将滚动均值映射回每日数据
    df['rolling_avg_eps'] = df['month'].map(monthly_rolling_avg_eps)

    df['shriller_pe'] = round(df['real_price'] / df['rolling_avg_eps'], 2)
    df.drop(columns=['rolling_avg_eps'], inplace=True)

    # 计算每个月的cpi，取每月第一天的值
    monthly_cpi = df.groupby('month')['cpi'].first()
    # 计算10年年化的cpi增长率：使用120个月数据
    monthly_cpi_growth = (monthly_cpi / monthly_cpi.shift(120)) ** (1/10) - 1
    # 将增长率映射回每日数据
    df['monthly_cpi_growth'] = df['month'].map(monthly_cpi_growth)

    df['excess_cape_yield'] = 1 / df['shriller_pe'] - (df['long_interest_rate_gs10'] / 100 - df['monthly_cpi_growth'])
    df.drop(columns=['monthly_cpi_growth', 'month'], inplace=True)

    return df
