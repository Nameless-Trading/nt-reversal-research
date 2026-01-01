import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import os
    import altair as alt
    import statsmodels.formula.api as smf
    alt.data_transformers.enable("vegafusion")
    return alt, pl, smf


@app.cell
def _():
    N_BINS = 5
    return (N_BINS,)


@app.cell
def _(pl):
    df_reversal = (
        pl.read_parquet("data/stock_returns.parquet")
        .sort("ticker", "date")
        .with_columns(
            pl.col("return")
            .log1p()
            .rolling_sum(21)
            .shift(1)
            .over("ticker")
            .alias("reversal")
        )
        .drop_nulls()
    )

    df_reversal
    return (df_reversal,)


@app.cell
def _(N_BINS, df_reversal, pl):
    labels = [str(i) for i in range(N_BINS)]

    df_bins = df_reversal.with_columns(
        pl.col("reversal").qcut(N_BINS, labels=labels).over("date").alias("bin")
    )

    df_bins
    return (df_bins,)


@app.cell
def _(N_BINS, df_bins, pl):
    df_portfolios = (
        df_bins.group_by("date", "bin")
        .agg(pl.col("return").mean())
        .sort("date", "bin")
        .pivot(index="date", on="bin", values="return")
        .with_columns(pl.col("0").sub(str(N_BINS - 1)).alias("spread (0-4)"))
        .unpivot(index="date", variable_name="portfolio", value_name="return")
        .sort("date", "portfolio")
    )

    df_portfolios
    return (df_portfolios,)


@app.cell
def _(df_portfolios, pl):
    df_cumulative_returns = df_portfolios.select(
        "date",
        "portfolio",
        pl.col("return")
        .log1p()
        .cum_sum()
        .mul(100)
        .over("portfolio")
        .alias("cumulative_return"),
    )

    df_cumulative_returns
    return (df_cumulative_returns,)


@app.cell
def _(alt, df_cumulative_returns):
    (
        alt.Chart(df_cumulative_returns)
        .mark_line()
        .encode(
            x=alt.X("date", title=""),
            y=alt.Y("cumulative_return", title="Cumulative Log Returns (%)"),
            color=alt.Color("portfolio", title="Portfolio"),
        )
    )
    return


@app.cell
def _(df_portfolios, pl):
    df_summary = (
        df_portfolios.group_by("portfolio")
        .agg(
            pl.col("return").mean().mul(252 * 100).alias("mean"),
            pl.col("return").std().mul(pl.lit(252).sqrt() * 100).alias("stdev"),
        )
        .with_columns(pl.col("mean").truediv(pl.col("stdev")).alias("sharpe"))
        .with_columns(pl.exclude("portfolio").round(2))
        .sort("portfolio")
    )

    df_summary
    return


@app.cell
def _(pl):
    df_factors = (
        pl.read_parquet("data/etf_returns.parquet")
        .sort("ticker")
        .pivot(index="date", on="ticker", values="return")
        .sort("date")
    )

    df_factors
    return (df_factors,)


@app.cell
def _(df_factors, df_portfolios, pl, smf):
    df_all_joined = (
        df_portfolios.join(other=df_factors, on="date", how="left")
        .rename({"return": "portfolio_return"})
        .with_columns(pl.exclude("date", "portfolio").mul(100 * 252))
        .sort("date", "portfolio")
    )

    results_list = []
    for portfolio_name in df_all_joined["portfolio"].unique().sort():
        df_portfolio = df_all_joined.filter(pl.col("portfolio").eq(portfolio_name))

        formula = "portfolio_return ~ MTUM + QUAL + SPY + USMV + VLUE"
        model = smf.ols(formula=formula, data=df_portfolio)
        result = model.fit()

        portfolio_results = pl.DataFrame(
            {
                "portfolio": portfolio_name,
                "parameter": result.params.index.tolist(),
                "B": result.params.values.tolist(),
                "T": result.tvalues.values.tolist(),
            }
        )

        results_list.append(portfolio_results)

    df_regression_results = (
        pl.concat(results_list)
        .pivot(index="portfolio", on="parameter", values=["B", "T"])
        .with_columns(pl.exclude("portfolio").round(2))
    )

    df_regression_results
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
