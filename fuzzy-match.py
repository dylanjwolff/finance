import polars as pl
import tlsh


df = pl.read_csv("historical_data.csv")
print(df.columns)
prediction_cols = ["Labels", "Category name"]
exact_match_cols = ["Amount", "Currency", "Author", "Note"]
exact_match_df = df.select(exact_match_cols + prediction_cols).unique()

def hash_row(row):
    return (tlsh.hash(bytes(str(row), "utf-8")))

exact_match_df = exact_match_df.with_columns([
    pl.struct(exact_match_cols).map_elements(hash_row).alias("hash")
])

new_df = pl.read_csv("new_historical_data.csv").tail(10)
new_df = new_df.with_columns([
    pl.struct(exact_match_cols).map_elements(hash_row).alias("hash")
])


# print(exact_match_df)
# print(new_df)

def find_min(row):
    h1 = row["hash"]
    diff = exact_match_df["hash"].map_elements(lambda h2: tlsh.diff(h1, h2)).alias("diff")
    match_row = exact_match_df[diff.arg_min()].select(prediction_cols)
    row_dict = dict(zip([f"pred_{c}" for c in match_row.columns], match_row.row(0)))
    row_dict["distance"] = diff.min()

    return row_dict


new_df = new_df.with_columns([
    pl.struct(["hash"]).map_elements(find_min).alias("predicted")
])


print(new_df.unnest("predicted"))
