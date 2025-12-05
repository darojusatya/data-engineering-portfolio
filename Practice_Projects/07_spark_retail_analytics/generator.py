# generates a sized CSV for analytics
import csv, random
rows = 20000
with open('data/orders_large.csv','w',newline='') as f:
    w = csv.writer(f)
    w.writerow(['order_id','customer_id','item','quantity','price','date'])
    for i in range(1,rows+1):
        w.writerow([i, random.randint(100,200), random.choice(['A','B','C','D']), random.randint(1,5), round(random.random()*100,2), '2025-11-01'])
print("generated", rows, "rows")
