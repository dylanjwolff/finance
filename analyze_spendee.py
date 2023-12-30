import polars as pl
import sys
import subprocess as sp
import os
import shutil
import glob
import forex_python

from forex_python.converter import CurrencyRates
exchange_rates = CurrencyRates()

base_currency = "SGD"
recode_dict = {
    "Taxi /  Rideshare": "Taxi"
}


def main():
    zip_archive_fnames = sys.argv[1:]
    df = parse_new_data(zip_archive_fnames)
    net_expenses = compute_net_expenses(df)
    print(net_expenses)
    print(net_expenses.sum().select("Net (SGD)"))
    df.write_csv("data/new_historical_data.csv")


def parse_new_data(zip_archive_fnames):
    dfs = []
    for zip_archive_fname in zip_archive_fnames:
        zip_archive_out = (zip_archive_fname.replace(".zip", "_out"))
        shutil.rmtree(zip_archive_out, ignore_errors=True)
        os.makedirs(zip_archive_out)

        sp.run(f"cp {zip_archive_fname} {zip_archive_out}; cd {zip_archive_out}; unzip {os.path.basename(zip_archive_fname)}; rm {os.path.basename(zip_archive_fname)}", shell=True)

        for csv_fname in glob.glob(f"{zip_archive_out}/*.csv"):
            df = pl.read_csv(csv_fname)
            df = df.with_columns(pl.col("Amount").cast(pl.Float64))
            dfs += [df]
            print(df.dtypes)
            print(df.columns)

    df = pl.concat(dfs)

    df = df.with_columns(
       pl.col("Date").str.to_datetime()
    )

    df = df.with_columns(pl.col("Category name").replace(recode_dict))

    rates = []
    for c in df["Currency"].unique():
        rates += [(c, exchange_rates.get_rate(c, base_currency))]
    rates = pl.DataFrame(rates, schema=["Currency", "Exchange Rate"])

    df = df.join(rates, on=["Currency"], how="left")
    df = df.with_columns(
        (pl.col("Amount")*pl.col("Exchange Rate")).alias("Amount (SGD)")
    )

    return df

def compute_net_expenses(df):
    shared = (df.filter(pl.col("Labels").str.contains("Shared")))
    owed = (df.filter(pl.col("Labels").str.contains("Sugar")))

    shared = shared.with_columns(pl.lit(0.5).alias("Split Ratio"))
    owed = owed.with_columns(pl.lit(1.0).alias("Split Ratio"))
    print(owed.columns)

    totals_shared = pl.concat([shared, owed]) \
        .group_by(["Category name", "Author", "Split Ratio"]) \
        .sum().select(["Category name", "Author", "Amount (SGD)", "Split Ratio"])


    totals_shared = totals_shared.with_columns(
        pl.when(~pl.col("Author").str.contains("Dylan"))
            .then(pl.col("Amount (SGD)")*pl.col("Split Ratio"))
            .otherwise(pl.col("Amount (SGD)")*pl.col("Split Ratio")*-1)
            .alias("Net (SGD)")
    )


    totals_shared = totals_shared.group_by(["Category name"]).sum() \
        .select(["Category name", "Net (SGD)"])

    return totals_shared

if __name__ == "__main__":
    main()
