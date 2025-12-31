from clients import get_clickhouse_client
import polars as pl
import datetime as dt
import os

os.makedirs("data", exist_ok=True)

clickhouse_client = get_clickhouse_client()

start = dt.date(2020, 7, 28)
end = dt.date(2025, 12, 29)

stock_returns_arrow = clickhouse_client.query_arrow(
    f"""
    SELECT
        u.date,
        u.ticker,
        s.return 
    FROM universe u
    LEFT JOIN stock_returns s ON u.date = s.date AND u.ticker = s.ticker 
    WHERE u.date BETWEEN '{start}' AND '{end}'
    """
)

stock_returns = (
    pl.from_arrow(stock_returns_arrow)
    .with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"))
    .sort("ticker", "date")
)

stock_returns.write_parquet("research/data/stock_returns.parquet")

etf_returns_arrow = clickhouse_client.query_arrow(
    f"SELECT * FROM etf_returns WHERE date BETWEEN '{start}' AND '{end}'"
)

etf_returns = (
    pl.from_arrow(etf_returns_arrow)
    .with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"))
    .sort("ticker", "date")
)

etf_returns.write_parquet("research/data/etf_returns.parquet")
