from clients import get_bear_lake_client
import polars as pl
import datetime as dt
import os
import bear_lake as bl

os.makedirs("data", exist_ok=True)

bear_lake_client = get_bear_lake_client()

start = dt.date(2020, 7, 28)
end = dt.date(2025, 12, 31)

stock_returns = (
    bear_lake_client.query(
        bl.table('universe')
        .join(
            other=bl.table('stock_returns'),
            on=['date', 'ticker'],
            how='left'
        )
        .filter(
            pl.col('date').is_between(start, end)
        )
        .select('date', 'ticker', 'return')
        .sort("ticker", "date")
    )
)
stock_returns.write_parquet("research/data/stock_returns.parquet")

etf_returns = (
    bear_lake_client.query(
        bl.table('etf_returns')
        .filter(
            pl.col('date').is_between(start, end)
        )
        .select('date', 'ticker', 'return')
        .sort('ticker', 'date')
    )
)
etf_returns.write_parquet("research/data/etf_returns.parquet")
