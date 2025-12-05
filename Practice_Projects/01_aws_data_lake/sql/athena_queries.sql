-- Athena style queries (point to curated parquet location)
SELECT sale_date, SUM(total) as daily_total
FROM curated_table
GROUP BY sale_date
ORDER BY sale_date DESC;
