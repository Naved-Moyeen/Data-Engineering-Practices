--Profit by Customer
SELECT
    customer_id,
    customer_name,
    SUM(profit) AS total_profit
FROM main.sales_gold.profit_by_year_category_subcat_customer
GROUP BY customer_id,customer_name
ORDER BY total_profit DESC