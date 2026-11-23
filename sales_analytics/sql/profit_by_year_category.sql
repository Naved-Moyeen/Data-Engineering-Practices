-- Profit by Year + Product Category
SELECT
    year,
    category,
    SUM(profit) AS total_profit
FROM main.sales_gold.profit_by_year_category_subcat_customer
GROUP BY year, category
ORDER BY year, category