-- silver: cleaned
with src as (select * from {{ ref('sales_raw') }})
select
  order_id,
  customer_id,
  quantity,
  price,
  quantity*price as total,
  to_date(date) as sale_date
from src
where quantity > 0
