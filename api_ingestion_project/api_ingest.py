import requests
import pandas as pd

def fetch():
    r = requests.get("https://jsonplaceholder.typicode.com/posts")
    return r.json()

def transform(data):
    df = pd.json_normalize(data)
    return df[['userId', 'id', 'title']]

def load(df, out):
    df.to_csv(out, index=False)

if __name__ == "__main__":
    data = fetch()
    df = transform(data)
    load(df, "posts.csv")
