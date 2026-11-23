"""Output schemas for the Silver and Gold tables."""
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DateType,
)

MONEY = DecimalType(10, 2)
MONEY_TOTAL = DecimalType(12, 2)


# ----------------------------------------------------------------------
# SILVER SCHEMAS
# ----------------------------------------------------------------------

SILVER_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),  # natural key, deduped
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", StringType(), True),  # string: keeps "07601"
    StructField("region", StringType(), True),
])


SILVER_PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), False),  # natural key, deduped
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("state", StringType(), True),
    StructField("unit_price", MONEY, True),  # 81.882 -> 81.88
])


SILVER_ORDERS_SCHEMA = StructType([
    StructField("order_line_id", IntegerType(), False),  # source "Row ID"
    StructField("order_id", StringType(), True),
    StructField("order_date", DateType(), True),  # day-first d/M/yyyy
    StructField("ship_date", DateType(), True),
    StructField("order_year", IntegerType(), True),  # derived from order_date
    StructField("ship_mode", StringType(), True),
    StructField("customer_id", StringType(), False),  # join key, normalized
    StructField("product_id", StringType(), False),  # join key, normalized
    StructField("quantity", IntegerType(), True),
    StructField("price", MONEY, True),
    StructField("discount", MONEY, True),
    StructField("profit", MONEY, True),  # rounded 2dp, negatives preserved
])


SILVER_ORDERS_MASTER_SCHEMA = StructType([
    # Order lines enriched with customer and product attributes.
    StructField("order_line_id", IntegerType(), False),
    StructField("order_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("ship_date", DateType(), True),
    StructField("order_year", IntegerType(), True),
    StructField("ship_mode", StringType(), True),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), True),
    StructField("price", MONEY, True),
    StructField("discount", MONEY, True),
    StructField("profit", MONEY, True),
    StructField("customer_name", StringType(), True),  # 'Unknown' on miss
    StructField("country", StringType(), True),        # 'Unknown' on miss
    StructField("category", StringType(), True),       # 'Unknown' on miss
    StructField("sub_category", StringType(), True),   # 'Unknown' on miss
])


# ----------------------------------------------------------------------
# GOLD SCHEMAS
# ----------------------------------------------------------------------

GOLD_PROFIT_SCHEMA = StructType([
    # Grain: year, category, sub-category and customer.
    StructField("year", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("profit", MONEY_TOTAL, True),
])


def conform_to_schema(df, schema):
    """Select the declared columns in schema order and cast their types."""
    columns = [
        (
            F.col(field.name).cast(field.dataType)
            if field.name in df.columns
            else F.lit(None).cast(field.dataType)
        ).alias(field.name)
        for field in schema.fields
    ]
    return df.select(*columns)
