"""Silver-layer cleaning, validation, quarantine and enrichment."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

try:
    from .schemas import (
        SILVER_CUSTOMERS_SCHEMA,
        SILVER_PRODUCTS_SCHEMA,
        SILVER_ORDERS_SCHEMA,
        SILVER_ORDERS_MASTER_SCHEMA,
        conform_to_schema,
    )
except ImportError:
    from schemas import (
        SILVER_CUSTOMERS_SCHEMA,
        SILVER_PRODUCTS_SCHEMA,
        SILVER_ORDERS_SCHEMA,
        SILVER_ORDERS_MASTER_SCHEMA,
        conform_to_schema,
    )

UNKNOWN = "Unknown"
DATE_FORMAT = "d/M/yyyy"
AUDIT_COLUMNS = {"source_file", "load_id", "ingestion_ts"}


def _rename_present(df: DataFrame, mapping: dict) -> DataFrame:
    return df.toDF(*[mapping.get(c, c) for c in df.columns])


def _nullif_blank(col):
    return F.when(F.trim(col.cast("string")) == "", None).otherwise(F.trim(col.cast("string")))


def _clean_display(df: DataFrame, cols) -> DataFrame:
    for col_name in cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, _nullif_blank(F.col(col_name)))
    return df


def _normalize_key(df: DataFrame, col_name: str) -> DataFrame:
    """Trim and uppercase identifiers; turn blank keys into NULL."""
    value = F.trim(F.col(col_name).cast("string"))
    return df.withColumn(
        col_name,
        F.when(value.isNull() | (value == ""), F.lit(None).cast("string"))
         .otherwise(F.upper(value)),
    )


def _require_columns(df: DataFrame, required, source_name: str) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(f"{source_name} source is missing required columns: {missing}")


def _reason_expression(reasons):
    parts = [F.when(condition, F.lit(code)) for condition, code in reasons]
    return F.concat_ws("|", *parts)


def _add_reason(df: DataFrame, reasons, existing_reason=None) -> DataFrame:
    reason = _reason_expression(reasons)
    if existing_reason is not None:
        reason = F.concat_ws("|", F.col(existing_reason), reason)
    return df.withColumn("_quarantine_reason", F.when(reason != "", reason))


def _deduplicate_by_business_key(df: DataFrame, key: str, conflict_reason: str):
    """Collapse exact copies and quarantine conflicting versions of a key.

    Ingestion metadata is excluded from row identity because the same source
    record can be loaded more than once with different audit values.
    """
    keyed = df.where(F.col(key).isNotNull())
    missing = df.where(F.col(key).isNull())
    identity_cols = [c for c in df.columns if c not in AUDIT_COLUMNS]
    distinct_rows = keyed.dropDuplicates(identity_cols)
    duplicate_count = keyed.count() - distinct_rows.count()

    conflict_keys = (
        distinct_rows.groupBy(key)
        .count()
        .where(F.col("count") > 1)
        .select(key)
    )
    conflicting = (
        distinct_rows.join(conflict_keys, key, "inner")
        .withColumn("_quarantine_reason", F.lit(conflict_reason))
    )
    unique = distinct_rows.join(conflict_keys, key, "left_anti")
    return unique, conflicting, missing, duplicate_count


def clean_customers(raw_df: DataFrame) -> tuple:
    """Clean customers and return clean rows, quarantine and duplicate count."""
    df = _rename_present(raw_df, {
        "Customer ID": "customer_id", "Customer Name": "customer_name",
        "Segment": "segment", "Country": "country", "City": "city",
        "State": "state", "Postal Code": "postal_code", "Region": "region",
    })
    _require_columns(df, ["customer_id"], "Customer")
    df = _normalize_key(df, "customer_id")
    df = _clean_display(df, ["customer_name", "email", "phone", "address", "segment", "country", "city", "state", "postal_code", "region"])

    unique, conflicts, missing, dropped = _deduplicate_by_business_key(
        df, "customer_id", "conflicting_duplicate_customer_id"
    )
    missing = _add_reason(missing, [(F.lit(True), "missing_customer_id")])
    quarantine = conflicts.unionByName(missing, allowMissingColumns=True)

    display = [c for c in [
        "customer_name", "email", "phone", "address", "segment", "country",
        "city", "state", "postal_code", "region",
    ] if c in unique.columns]
    unique = _clean_display(unique, display)
    quarantine = _clean_display(quarantine, display)
    clean = conform_to_schema(unique, SILVER_CUSTOMERS_SCHEMA)
    return clean, quarantine, dropped


def clean_products(raw_df: DataFrame) -> tuple:
    """Return (clean products, quarantined products, exact duplicates dropped).

    Exact duplicate product rows collapse to one. Every distinct row for a
    product_id with conflicting attributes is quarantined.
    """
    df = _rename_present(raw_df, {
        "Product ID": "product_id", "Category": "category",
        "Sub-Category": "sub_category", "Product Name": "product_name",
        "State": "state", "Price per product": "unit_price",
    })
    _require_columns(df, ["product_id", "category", "sub_category", "unit_price"], "Products")
    df = _normalize_key(df, "product_id")
    df = _clean_display(df, ["category", "sub_category", "product_name", "state"])

    raw_price = F.col("unit_price")
    parsed_price = F.expr(
        "try_cast(round(try_cast(cast(`unit_price` as string) as decimal(18,6)), 2) as decimal(10,2))"
    )
    unrounded_unit_price = F.expr("try_cast(cast(`unit_price` as string) as decimal(18,6))")
    df = df.withColumn(
        "__identity_unit_price",
        F.coalesce(unrounded_unit_price.cast("string"), raw_price.cast("string")),
    )
    df = df.withColumn("__invalid_unit_price", raw_price.isNull() | parsed_price.isNull())
    df = df.withColumn("unit_price", parsed_price)

    unique, conflicts, missing, dropped = _deduplicate_by_business_key(
        df, "product_id", "conflicting_duplicate_product_id"
    )
    invalid_reasons = [
        (F.col("category").isNull(), "missing_category"),
        (F.col("sub_category").isNull(), "missing_sub_category"),
        (F.col("__invalid_unit_price"), "invalid_unit_price"),
    ]
    invalid_product = invalid_reasons[0][0] | invalid_reasons[1][0] | invalid_reasons[2][0]
    invalid = _add_reason(unique.where(invalid_product), invalid_reasons)
    clean = unique.where(~invalid_product)
    conflicts = _add_reason(conflicts, invalid_reasons, "_quarantine_reason")
    missing = _add_reason(
        missing,
        [(F.lit(True), "missing_product_id")] + invalid_reasons,
    )

    temp_cols = ["__invalid_unit_price", "__identity_unit_price"]
    clean = clean.drop(*temp_cols)
    quarantine = conflicts.unionByName(invalid.drop(*temp_cols), allowMissingColumns=True)
    quarantine = quarantine.unionByName(missing.drop(*temp_cols), allowMissingColumns=True)
    clean = conform_to_schema(clean, SILVER_PRODUCTS_SCHEMA)
    return clean, quarantine, dropped


def clean_orders(raw_df: DataFrame) -> tuple:
    """Clean orders and return clean rows, quarantine and duplicate count."""
    df = _rename_present(raw_df, {
        "Row ID": "order_line_id", "Order ID": "order_id",
        "Order Date": "order_date", "Ship Date": "ship_date",
        "Ship Mode": "ship_mode", "Customer ID": "customer_id",
        "Product ID": "product_id", "Quantity": "quantity",
        "Price": "price", "Discount": "discount", "Profit": "profit",
    })
    required = [
        "order_line_id", "order_date", "ship_date", "customer_id", "product_id",
        "quantity", "price", "discount", "profit",
    ]
    _require_columns(df, required, "Orders")

    # Preserve invalid source date text for duplicate comparison.
    for col_name in ["order_date", "ship_date"]:
        raw_date = F.trim(F.col(col_name).cast("string"))
        df = df.withColumn(f"__raw_{col_name}", raw_date)
        df = df.withColumn(
            col_name,
            F.expr(
                f"cast(try_to_timestamp(trim(cast(`{col_name}` as string)), '{DATE_FORMAT}') as date)"
            ),
        )
        df = df.withColumn(
            f"__identity_{col_name}",
            F.coalesce(F.col(col_name).cast("string"), F.col(f"__raw_{col_name}")),
        ).drop(f"__raw_{col_name}")

    for col_name in ["customer_id", "product_id"]:
        df = _normalize_key(df, col_name)
    if "ship_mode" in df.columns:
        df = _clean_display(df, ["ship_mode"])

    raw_line_id = F.col("order_line_id")
    line_id_decimal = F.expr("try_cast(cast(`order_line_id` as string) as decimal(18,6))")
    parsed_line_id = F.expr("try_cast(cast(`order_line_id` as string) as int)")
    df = df.withColumn(
        "__invalid_order_line_id",
        raw_line_id.isNull() | line_id_decimal.isNull() | parsed_line_id.isNull() |
        (parsed_line_id <= 0) | (line_id_decimal != parsed_line_id.cast("decimal(18,6)")),
    )
    df = df.withColumn("order_line_id", parsed_line_id)

    raw_quantity = F.col("quantity")
    quantity_decimal = F.expr("try_cast(cast(`quantity` as string) as decimal(18,6))")
    parsed_quantity = F.expr("try_cast(cast(`quantity` as string) as int)")
    df = df.withColumn(
        "__invalid_quantity",
        raw_quantity.isNull() | quantity_decimal.isNull() | parsed_quantity.isNull() |
        (parsed_quantity <= 0) | (quantity_decimal != parsed_quantity.cast("decimal(18,6)")),
    )
    df = df.withColumn(
        "__identity_quantity",
        F.coalesce(quantity_decimal.cast("string"), raw_quantity.cast("string")),
    )
    df = df.withColumn("quantity", parsed_quantity)

    for col_name in ["price", "discount", "profit"]:
        raw_value = F.col(col_name)
        parsed_value = F.expr(
            f"try_cast(round(try_cast(cast(`{col_name}` as string) as decimal(18,6)), 2) as decimal(10,2))"
        )
        unrounded_value = F.expr(f"try_cast(cast(`{col_name}` as string) as decimal(18,6))")
        invalid = raw_value.isNull() | parsed_value.isNull() | unrounded_value.isNull()
        if col_name == "price":
            invalid = invalid | (unrounded_value < 0)
        elif col_name == "discount":
            invalid = invalid | (unrounded_value < 0) | (unrounded_value > 1)
        df = df.withColumn(
            f"__identity_{col_name}",
            F.coalesce(unrounded_value.cast("string"), raw_value.cast("string")),
        )
        df = df.withColumn(f"__invalid_{col_name}", invalid)
        df = df.withColumn(col_name, parsed_value)

    if "order_date" in df.columns:
        df = df.withColumn("order_year", F.year(F.col("order_date")).cast("int"))

    unique, conflicts, missing_line_id, dropped = _deduplicate_by_business_key(
        df, "order_line_id", "conflicting_duplicate_order_line_id"
    )

    common_reasons = [
        (F.col("order_date").isNull(), "invalid_order_date"),
        (F.col("ship_date").isNull(), "invalid_ship_date"),
        (F.col("customer_id").isNull(), "missing_customer_id"),
        (F.col("product_id").isNull(), "missing_product_id"),
        (F.col("__invalid_order_line_id"), "invalid_order_line_id"),
        (F.col("__invalid_quantity"), "invalid_quantity"),
        (F.col("__invalid_price"), "invalid_price"),
        (F.col("__invalid_discount"), "invalid_discount"),
        (F.col("__invalid_profit"), "invalid_profit"),
    ]
    fail = F.lit(False)
    for condition, _ in common_reasons:
        fail = fail | condition

    invalid = _add_reason(unique.where(fail), common_reasons)
    clean = unique.where(~fail)
    conflicts = _add_reason(conflicts, common_reasons, "_quarantine_reason")
    missing_line_id = _add_reason(missing_line_id, common_reasons)

    temp_cols = [c for c in df.columns if c.startswith("__invalid_") or c.startswith("__identity_")]
    clean = conform_to_schema(clean.drop(*temp_cols), SILVER_ORDERS_SCHEMA)
    quarantine = (
        conflicts.drop(*temp_cols)
        .unionByName(invalid.drop(*temp_cols), allowMissingColumns=True)
        .unionByName(missing_line_id.drop(*temp_cols), allowMissingColumns=True)
    )
    return clean, quarantine, dropped


def _assert_unique_keys(df: DataFrame, key: str, name: str) -> None:
    dupes = df.groupBy(key).count().where(F.col("count") > 1).limit(1).collect()
    if dupes:
        raise ValueError(f"{name} dimension has duplicate {key} values")


def build_orders_master(orders_df: DataFrame, customers_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """Enrich order lines using left joins to the cleaned dimensions."""
    _assert_unique_keys(customers_df, "customer_id", "customers")
    _assert_unique_keys(products_df, "product_id", "products")

    customer_cols = [c for c in ["customer_id", "customer_name", "country"] if c in customers_df.columns]
    product_cols = [c for c in ["product_id", "category", "sub_category"] if c in products_df.columns]
    master = (
        orders_df
        .join(F.broadcast(customers_df.select(*customer_cols)), "customer_id", "left")
        .join(F.broadcast(products_df.select(*product_cols)), "product_id", "left")
    )
    for col_name in ["customer_name", "country", "category", "sub_category"]:
        if col_name in master.columns:
            master = master.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(UNKNOWN)))
    return conform_to_schema(master, SILVER_ORDERS_MASTER_SCHEMA)
