"""Run the Bronze, Silver and Gold batch pipeline."""
from pyspark.sql import DataFrame, SparkSession

try:
    from .bronze import run_bronze_load
    from .silver import build_orders_master, clean_customers, clean_orders, clean_products
    from .gold import aggregate_profit
except ImportError:
    from bronze import run_bronze_load
    from silver import build_orders_master, clean_customers, clean_orders, clean_products
    from gold import aggregate_profit

BRONZE_SCHEMA = "main.sales_bronze"
SILVER_SCHEMA = "main.sales_silver"
GOLD_SCHEMA = "main.sales_gold"
GOLD_TABLE = f"{GOLD_SCHEMA}.profit_by_year_category_subcat_customer"


def _overwrite(df: DataFrame, table: str) -> int:
    """Write a Delta snapshot and return its row count."""
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(table))
    return df.sparkSession.table(table).count()


def run_silver_pipeline(spark: SparkSession) -> dict:
    """Rebuild Silver tables and check source-to-target row counts."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

    customers, customers_bad, customers_deduped = clean_customers(
        spark.table(f"{BRONZE_SCHEMA}.customer")
    )
    products, products_bad, products_deduped = clean_products(
        spark.table(f"{BRONZE_SCHEMA}.products")
    )
    orders, orders_bad, orders_deduped = clean_orders(
        spark.table(f"{BRONZE_SCHEMA}.orders")
    )
    master = build_orders_master(orders, customers, products)

    summary = {
        "customers": _overwrite(customers, f"{SILVER_SCHEMA}.customers"),
        "customers_quarantine": _overwrite(customers_bad, f"{SILVER_SCHEMA}.customers_quarantine"),
        "customers_exact_duplicates_dropped": customers_deduped,
        "products": _overwrite(products, f"{SILVER_SCHEMA}.products"),
        "products_quarantine": _overwrite(products_bad, f"{SILVER_SCHEMA}.products_quarantine"),
        "products_exact_duplicates_dropped": products_deduped,
        "orders": _overwrite(orders, f"{SILVER_SCHEMA}.orders"),
        "orders_quarantine": _overwrite(orders_bad, f"{SILVER_SCHEMA}.orders_quarantine"),
        "orders_exact_duplicates_dropped": orders_deduped,
        "orders_master": _overwrite(master, f"{SILVER_SCHEMA}.orders_master"),
    }

    checks = [
        ("customer", "customers", "customers_quarantine", customers_deduped),
        ("products", "products", "products_quarantine", products_deduped),
        ("orders", "orders", "orders_quarantine", orders_deduped),
    ]
    for source, clean_key, quarantine_key, duplicates in checks:
        source_count = spark.table(f"{BRONZE_SCHEMA}.{source}").count()
        accounted = summary[clean_key] + summary[quarantine_key] + duplicates
        if accounted != source_count:
            raise ValueError(
                f"{source} reconciliation failed: clean={summary[clean_key]}, "
                f"quarantine={summary[quarantine_key]}, exact_duplicates={duplicates}, "
                f"bronze={source_count}"
            )

    return summary


def run_gold_pipeline(spark: SparkSession) -> dict:
    """Rebuild the Gold profit aggregate."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    gold = aggregate_profit(spark.table(f"{SILVER_SCHEMA}.orders_master"))
    return {"gold_rows": _overwrite(gold, GOLD_TABLE)}


def run_pipeline(spark: SparkSession) -> dict:
    """Run a full source-to-Gold refresh."""
    return {
        "bronze": run_bronze_load(spark),
        "silver": run_silver_pipeline(spark),
        "gold": run_gold_pipeline(spark),
    }
