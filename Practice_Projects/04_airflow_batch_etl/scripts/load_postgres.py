# This script would load cleaned parquet into Postgres. Here it writes a CSV for demo.
import pandas as pd
df = pd.read_parquet("data/clean.parquet")
df.to_csv("data/load_ready.csv", index=False)
print("ready to load to Postgres")
