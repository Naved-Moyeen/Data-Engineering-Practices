--Profit by Year
SELECT
    year,
    SUM(profit) AS total_profit
FROM main.sales_gold.profit_by_year_category_subcat_customer
GROUP BY year
ORDER BY year