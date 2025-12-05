# simple wrapper that mimics running a GE validation on CSV using pandas
import pandas as pd
df = pd.read_csv("data/sales_small.csv")
assert df.shape[0] > 0, "row count 0"
assert df['order_id'].notnull().all(), "null order_id"
print("validation passed (simple mimic). For full GE, initialize Great Expectations and run suites.")
