-- Example SQL transformations
SELECT customer_id, SUM(total) AS total_spent
FROM clean_sales
GROUP BY customer_id;
