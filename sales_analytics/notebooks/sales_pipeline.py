# Databricks notebook source
from pathlib import Path
import sys

current = Path.cwd().resolve()
project_root = next(
    (path for path in (current, current.parent) if (path / "src" / "main.py").is_file()),
    None,
)
if project_root is None:
    raise RuntimeError("Could not find the project root. Add it to sys.path before running this notebook.")

sys.path.insert(0, str(project_root))
from src.main import run_pipeline

pipeline_summary = run_pipeline(spark)
print(pipeline_summary)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year, SUM(profit) AS total_profit
# MAGIC FROM main.sales_gold.profit_by_year_category_subcat_customer
# MAGIC GROUP BY year
# MAGIC ORDER BY year;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year, category, SUM(profit) AS total_profit
# MAGIC FROM main.sales_gold.profit_by_year_category_subcat_customer
# MAGIC GROUP BY year, category
# MAGIC ORDER BY year, category;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, customer_name, SUM(profit) AS total_profit
# MAGIC FROM main.sales_gold.profit_by_year_category_subcat_customer
# MAGIC GROUP BY customer_id, customer_name
# MAGIC ORDER BY total_profit DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year, customer_id, customer_name, SUM(profit) AS total_profit
# MAGIC FROM main.sales_gold.profit_by_year_category_subcat_customer
# MAGIC GROUP BY year, customer_id, customer_name
# MAGIC ORDER BY year, total_profit DESC;
