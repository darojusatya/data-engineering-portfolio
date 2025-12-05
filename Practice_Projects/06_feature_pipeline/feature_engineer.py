import pandas as pd
df = pd.read_csv("data/raw_customers.csv")
# example features
df['avg_order_value'] = df['total_spent'] / df['order_count']
feats = df[['customer_id','avg_order_value','order_count']]
feats.to_parquet("features/features.parquet", index=False)
print("features written")
