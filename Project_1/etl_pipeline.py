import pandas as pd

def extract(path):
    return pd.read_csv(path)

def transform(df):
    df['total'] = df['quantity'] * df['price']
    df = df[df['quantity'] > 0]
    return df

def load(df, out_path):
    df.to_csv(out_path, index=False)

if __name__ == "__main__":
    df = extract("data/raw_sales.csv")
    df_t = transform(df)
    load(df_t, "data/clean_sales.csv")
