"""Gold layer: pre-aggregated profit table for serving.

aggregate_profit is a pure function : master in, gold grain out.
The grain is year x category x sub-category x customer (keyed on
customer_id, name carried for readability). Profit is summed AFTER
per-line 2dp rounding  — so every roll-up of the
gold table reconciles exactly with the master total .

Output is anchored to GOLD_PROFIT_SCHEMA in src/schemas.py.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

try:
    from .schemas import GOLD_PROFIT_SCHEMA, conform_to_schema
except ImportError:  # allow flat import when src/ is on sys.path directly
    from schemas import GOLD_PROFIT_SCHEMA, conform_to_schema


def aggregate_profit(master_df: DataFrame) -> DataFrame:
    """Silver orders_master -> gold profit table.

    Groups by (order_year, category, sub_category, customer_id,
    customer_name) and sums the already-2dp line profits. Renames
    order_year -> year to match the Gold table schema.
    """
    gold = (
        master_df
        .groupBy("order_year", "category", "sub_category", "customer_id", "customer_name")
        .agg(F.sum("profit").alias("profit"))
        .withColumnRenamed("order_year", "year")
    )
    return conform_to_schema(gold, GOLD_PROFIT_SCHEMA)
