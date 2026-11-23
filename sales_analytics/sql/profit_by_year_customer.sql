-- Profit by Customer + Year
SELECT
    year,
    customer_id,
    customer_name,
    SUM(profit) AS total_profit
FROM main.sales_gold.profit_by_year_category_subcat_customer
GROUP BY year, customer_id, customer_name
ORDER BY year, total_profit DESC