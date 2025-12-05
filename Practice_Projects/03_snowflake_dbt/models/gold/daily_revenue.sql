select sale_date, sum(total) as daily_revenue
from {{ ref('sales_clean') }}
group by sale_date
