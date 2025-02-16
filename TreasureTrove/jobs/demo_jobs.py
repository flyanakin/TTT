from dagster import define_asset_job, AssetSelection

tushare_default_config = {
    "ops": {
        "stock_warehouse__sources__tushare__index_daily": {
            "config": {
                "mode": "increment",
                "code": "",
                "start_date": "",
                "end_date": "",
            }
        }
    }
}

portfolio_job = define_asset_job(
    name='portfolio_job',
    selection=AssetSelection.keys(['stock_warehouse', 'marts', 'portfolio'],
                                  ['stock_warehouse', 'staging', 'stg_transactions']).upstream(),
)


financial_db_update_job = define_asset_job(
    name='financial_db_update_job',
    selection=AssetSelection.keys(['stock_warehouse', 'sources', 'tushare', 'index_daily']).downstream(),
    config=tushare_default_config
)