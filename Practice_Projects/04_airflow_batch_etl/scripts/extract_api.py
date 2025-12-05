# Simulated API extraction - writes to data/raw.json
import json
data = [{"order_id":1,"customer_id":100,"quantity":2,"price":10.5,"date":"2025-11-01"}]
with open("data/raw.json","w") as f:
    json.dump(data,f)
print("extracted")
