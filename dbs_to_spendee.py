import polars as pl
import glob


def read_dbs(fname):
    write = False
    with open(fname) as f:
        lines = f.readlines()

    if not lines[0].startswith("Transaction"):
        with open(fname, "w") as tf:
            for line in lines:
                if len(line) == 0 or ',' not in line:
                    continue
                if line.startswith("Transaction"):
                    write = True
                if write:
                    tf.write(line)

    df = pl.read_csv(fname, truncate_ragged_lines=True, schema_overrides={"Debit Amount": pl.Float64,"Credit Amount": pl.Float64})
    df = df.with_columns(pl.col("Transaction Date").str.to_date(format="%d %B %Y")) \
            .with_columns(pl.col("Value Date").str.to_date(format="%d %B %Y"))
    return df

dfs = [read_dbs(fname) for fname in glob.glob("dbs*.csv")]
df = pl.concat(dfs)

assert(df.filter(pl.col("Debit Amount").is_not_null() & pl.col("Credit Amount").is_not_null()).height == 0)

df_d = df.filter(pl.col("Debit Amount").is_not_null()).with_columns(
            (pl.col("Debit Amount") * -1).alias("Amount"),
            pl.lit("Income").alias("Type")
        )
df_c = df.filter(pl.col("Credit Amount").is_not_null()).with_columns(
            (pl.col("Credit Amount")).alias("Amount"),
            pl.lit("Expense").alias("Type")
        )
df = pl.concat([df_d, df_c])


df = df.rename({
    "Transaction Date": "Date",
    "Client Reference": "Note",
    }).with_columns(
            pl.lit("Dylan Wolff").alias("Author"),
            pl.lit("Currency").alias("SGD"),
            pl.lit(1).alias("Exchange Rate"),
            pl.lit(None).alias("Category name"),
            pl.lit(None).alias("Labels"),
            pl.lit("120-935082-9 SGD SGD").alias("Wallet"),
            pl.lit("SGD").alias("Currency"),
            pl.col("Amount").alias("Amount (SGD)")
        )
df = df.select([
        "Date","Wallet","Type","Category name","Amount","Currency","Note","Labels","Author","Exchange Rate","Amount (SGD)"
    ])

df.write_csv("new_dbs.csv")
print(df)

