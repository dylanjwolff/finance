import polars as pl
import sys
import subprocess as sp
import os
import shutil
import glob

zip_archive_fname = sys.argv[1]

zip_archive_out = (zip_archive_fname.replace(".zip", "_out"))
shutil.rmtree(zip_archive_out)
os.makedirs(zip_archive_out)

sp.run(f"cp {zip_archive_fname} {zip_archive_out}; cd {zip_archive_out}; unzip {os.path.basename(zip_archive_fname)}; rm {os.path.basename(zip_archive_fname)}", shell=True)

dfs = []
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

shared = (df.filter(pl.col("Labels").str.contains("Shared") ))

totals_shared = shared \
    .group_by(["Category name", "Currency", "Author"]) \
    .sum().select(["Category name", "Currency", "Author", "Amount"])
print(totals_shared)
