"""Tests for Silver transformations, key handling and quarantine rules."""
from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from src.silver import (
    build_orders_master,
    clean_customers,
    clean_orders,
    clean_products,
)


CUSTOMERS_ROWS = [
    {"Customer ID": " jk-15370 ", "Customer Name": "  John Doe  ", "Country": "United States", "Postal Code": "07601"},
    {"Customer ID": "JK-15370", "Customer Name": "John Doe", "Country": "United States", "Postal Code": "07601"},
    {"Customer ID": "BD-11320", "Customer Name": "", "Country": "United States", "Postal Code": "1852"},
]


def _raw_customers(spark, rows=CUSTOMERS_ROWS):
    return spark.createDataFrame([Row(**r) for r in rows])


def test_customers_normalize_key_upper_and_trim(spark):
    clean, quarantine, dropped = clean_customers(_raw_customers(spark))
    assert clean.filter("customer_id = 'JK-15370'").count() == 1
    assert dropped == 1
    assert quarantine.count() == 0


def test_customers_empty_display_string_becomes_null(spark):
    clean, _, _ = clean_customers(_raw_customers(spark))
    row = clean.filter("customer_id = 'BD-11320'").first()
    assert row.customer_name is None


def test_customers_postal_code_stays_string(spark):
    clean, _, _ = clean_customers(_raw_customers(spark))
    row = clean.filter("customer_id = 'JK-15370'").first()
    assert row.postal_code == "07601"


def test_customers_exact_duplicates_ignore_ingestion_metadata(spark):
    rows = [
        {"Customer ID": "C-1", "Customer Name": "Alice", "source_file": "customers_a.xlsx", "load_id": "load-1"},
        {"Customer ID": " c-1 ", "Customer Name": " Alice ", "source_file": "customers_b.xlsx", "load_id": "load-2"},
    ]
    clean, quarantine, dropped = clean_customers(_raw_customers(spark, rows))
    assert clean.count() == 1
    assert quarantine.count() == 0
    assert dropped == 1


def test_customers_conflicting_duplicate_key_is_quarantined(spark):
    rows = [
        {"Customer ID": "ab-1", "Customer Name": "Alice"},
        {"Customer ID": " AB-1 ", "Customer Name": "Alicia"},
    ]
    clean, quarantine, dropped = clean_customers(_raw_customers(spark, rows))
    assert clean.count() == 0
    assert quarantine.count() == 2
    assert quarantine.select("_quarantine_reason").distinct().first()[0] == "conflicting_duplicate_customer_id"
    assert dropped == 0



def test_customers_missing_key_column_fails(spark):
    df = spark.createDataFrame([Row(**{"Customer Name": "Alice"})])
    with pytest.raises(ValueError, match="customer_id"):
        clean_customers(df)

def test_customers_missing_key_is_quarantined(spark):
    clean, quarantine, dropped = clean_customers(_raw_customers(spark, [
        {"Customer ID": " ", "Customer Name": "No ID"},
    ]))
    assert clean.count() == 0
    assert quarantine.first()["_quarantine_reason"] == "missing_customer_id"
    assert dropped == 0


PRODUCTS_ROWS = [
    {"Product ID": "fur-ch-1", "Category": "Furniture", "Sub-Category": "Chairs", "Product Name": "  Leather Chair ", "Price per product": 81.882},
    {"Product ID": " FUR-CH-1 ", "Category": "Furniture", "Sub-Category": "Chairs", "Product Name": "Leather Chair", "Price per product": 81.882},
]


def _raw_products(spark, rows=PRODUCTS_ROWS):
    return spark.createDataFrame([Row(**r) for r in rows])


def test_products_exact_duplicate_rows_collapse(spark):
    clean, quarantine, dropped = clean_products(_raw_products(spark))
    assert clean.count() == 1
    assert clean.first().product_id == "FUR-CH-1"
    assert clean.first().unit_price == Decimal("81.88")
    assert quarantine.count() == 0
    assert dropped == 1


def test_products_conflicting_duplicate_key_quarantines_all_variants(spark):
    rows = [
        {"Product ID": "FUR-CH-1", "Category": "Furniture", "Sub-Category": "Chairs", "Product Name": "Chair A", "Price per product": 81.88},
        {"Product ID": "fur-ch-1", "Category": "Furniture", "Sub-Category": "Chairs", "Product Name": "Chair B", "Price per product": 99.99},
    ]
    clean, quarantine, dropped = clean_products(_raw_products(spark, rows))
    assert clean.count() == 0
    assert quarantine.count() == 2
    assert "conflicting_duplicate_product_id" in quarantine.first()["_quarantine_reason"]
    assert dropped == 0


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("Price per product", "not-a-price", "invalid_unit_price"),
        ("Category", "   ", "missing_category"),
        ("Sub-Category", "", "missing_sub_category"),
    ],
)
def test_products_invalid_records_are_quarantined(spark, field, value, reason):
    row = {"Product ID": "P-1", "Category": "Furniture", "Sub-Category": "Chairs", "Product Name": "Chair", "Price per product": 10.0}
    row[field] = value
    clean, quarantine, _ = clean_products(_raw_products(spark, [row]))
    assert clean.count() == 0
    assert reason in quarantine.first()["_quarantine_reason"]



def test_products_accept_new_nonblank_category_values(spark):
    row = {
        "Product ID": "P-NEW",
        "Category": "Home & Garden",
        "Sub-Category": "Outdoor Lighting",
        "Product Name": "Garden Light",
        "Price per product": 25.0,
    }
    clean, quarantine, _ = clean_products(_raw_products(spark, [row]))
    assert clean.count() == 1
    assert quarantine.count() == 0
    result = clean.first()
    assert result.category == "Home & Garden"
    assert result.sub_category == "Outdoor Lighting"


def test_products_missing_required_column_fails(spark):
    df = spark.createDataFrame([Row(**{"Product ID": "P-1", "Category": "Furniture"})])
    with pytest.raises(ValueError, match="sub_category|unit_price"):
        clean_products(df)

def test_products_missing_key_is_quarantined(spark):
    row = {"Product ID": " ", "Category": "Furniture", "Sub-Category": "Chairs", "Product Name": "Chair", "Price per product": 10.0}
    clean, quarantine, _ = clean_products(_raw_products(spark, [row]))
    assert clean.count() == 0
    assert quarantine.first()["_quarantine_reason"] == "missing_product_id"


ORDERS_ROWS = [
    {"Row ID": 1, "Order ID": "CA-2016-122581", "Order Date": "21/8/2016", "Ship Date": "25/8/2016",
     "Ship Mode": "Standard Class", "Customer ID": " jk-15370 ", "Product ID": "fur-ch-10002961",
     "Quantity": 7, "Price": 573.17, "Discount": 0.3, "Profit": 123.456},
    {"Row ID": 2, "Order ID": "US-2016-157490", "Order Date": "6/10/2016", "Ship Date": "7/10/2016",
     "Ship Mode": "First Class", "Customer ID": "LB-16795", "Product ID": "OFF-BI-10002824",
     "Quantity": 4, "Price": 17.0, "Discount": 0.7, "Profit": -14.92},
    {"Row ID": 3, "Order ID": "BAD-1", "Order Date": "not-a-date", "Ship Date": "25/8/2016",
     "Ship Mode": "Standard Class", "Customer ID": "JK-15370", "Product ID": "FUR-CH-10002961",
     "Quantity": 2, "Price": 100.0, "Discount": 0.0, "Profit": 10.0},
]


def _raw_orders(spark, rows=ORDERS_ROWS):
    return spark.createDataFrame([Row(**r) for r in rows])


def test_orders_parse_dates_and_normalize_keys(spark):
    clean, _, _ = clean_orders(_raw_orders(spark))
    row = clean.filter("order_line_id = 1").first()
    assert row.order_date == date(2016, 8, 21)
    assert row.ship_date == date(2016, 8, 25)
    assert row.order_year == 2016
    assert row.customer_id == "JK-15370"
    assert row.product_id == "FUR-CH-10002961"



def test_orders_missing_required_column_fails(spark):
    row = dict(ORDERS_ROWS[0])
    row.pop("Profit")
    with pytest.raises(ValueError, match="profit"):
        clean_orders(_raw_orders(spark, [row]))

def test_orders_invalid_dates_are_quarantined(spark):
    clean, quarantine, _ = clean_orders(_raw_orders(spark))
    assert clean.filter("order_line_id = 3").count() == 0
    row = quarantine.filter("order_line_id = 3").first()
    assert "invalid_order_date" in row._quarantine_reason


def test_orders_profit_rounding_and_negative_profit(spark):
    clean, _, _ = clean_orders(_raw_orders(spark))
    assert clean.filter("order_line_id = 1").first().profit == Decimal("123.46")
    assert clean.filter("order_line_id = 2").first().profit == Decimal("-14.92")


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("Quantity", "many", "invalid_quantity"),
        ("Quantity", 0, "invalid_quantity"),
        ("Quantity", 2.5, "invalid_quantity"),
        ("Price", "not-money", "invalid_price"),
        ("Price", -1, "invalid_price"),
        ("Discount", "unknown", "invalid_discount"),
        ("Discount", 1.5, "invalid_discount"),
        ("Profit", "not-money", "invalid_profit"),
    ],
)
def test_orders_invalid_numeric_values_are_quarantined(spark, field, value, reason):
    row = dict(ORDERS_ROWS[0])
    row["Row ID"] = 100
    row[field] = value
    clean, quarantine, _ = clean_orders(_raw_orders(spark, [row]))
    assert clean.count() == 0
    assert reason in quarantine.first()["_quarantine_reason"]


def test_orders_exact_duplicate_lines_collapse_and_are_counted(spark):
    row = dict(ORDERS_ROWS[0])
    clean, quarantine, dropped = clean_orders(_raw_orders(spark, [row, dict(row)]))
    assert clean.count() == 1
    assert quarantine.count() == 0
    assert dropped == 1
    assert clean.count() + quarantine.count() + dropped == 2


def test_orders_conflicting_duplicate_line_ids_quarantine_all_variants(spark):
    first = dict(ORDERS_ROWS[0])
    second = dict(first)
    second["Profit"] = 999.0
    clean, quarantine, dropped = clean_orders(_raw_orders(spark, [first, second]))
    assert clean.count() == 0
    assert quarantine.count() == 2
    assert "conflicting_duplicate_order_line_id" in quarantine.first()["_quarantine_reason"]
    assert dropped == 0


def test_orders_fractional_order_line_id_is_quarantined(spark):
    row = dict(ORDERS_ROWS[0])
    row["Row ID"] = 1.5
    clean, quarantine, _ = clean_orders(_raw_orders(spark, [row]))
    assert clean.count() == 0
    assert "invalid_order_line_id" in quarantine.first()["_quarantine_reason"]


def test_orders_missing_join_keys_are_quarantined(spark):
    row = dict(ORDERS_ROWS[0])
    row["Row ID"] = 101
    row["Customer ID"] = " "
    clean, quarantine, _ = clean_orders(_raw_orders(spark, [row]))
    assert clean.count() == 0
    assert "missing_customer_id" in quarantine.first()["_quarantine_reason"]


def test_orders_reconciliation_includes_quarantine_and_exact_duplicates(spark):
    rows = [dict(ORDERS_ROWS[0]), dict(ORDERS_ROWS[0]), dict(ORDERS_ROWS[2])]
    clean, quarantine, dropped = clean_orders(_raw_orders(spark, rows))
    assert clean.count() + quarantine.count() + dropped == len(rows)


def _silver_orders(spark):
    return spark.createDataFrame([
        Row(order_line_id=1, order_id="CA-2016-122581", order_date="2016-08-21",
            order_year=2016, ship_mode="Standard Class", customer_id="JK-15370",
            product_id="FUR-CH-10002961", quantity=7, price=Decimal("573.17"),
            discount=Decimal("0.30"), profit=Decimal("63.69")),
        Row(order_line_id=2, order_id="XX-2016-999999", order_date="2016-10-06",
            order_year=2016, ship_mode="First Class", customer_id="NO-SUCH-CUST",
            product_id="NO-SUCH-PROD", quantity=4, price=Decimal("17.00"),
            discount=Decimal("0.70"), profit=Decimal("-14.92")),
    ])


def _silver_customers(spark):
    return spark.createDataFrame([Row(customer_id="JK-15370", customer_name="John Doe", country="United States")])


def _silver_products(spark):
    return spark.createDataFrame([Row(product_id="FUR-CH-10002961", category="Furniture", sub_category="Chairs")])


def test_master_enriches_customer_and_product_attributes(spark):
    master = build_orders_master(_silver_orders(spark), _silver_customers(spark), _silver_products(spark))
    row = master.filter("order_line_id = 1").first()
    assert (row.customer_name, row.country, row.category, row.sub_category) == (
        "John Doe", "United States", "Furniture", "Chairs"
    )


def test_master_unmatched_dimensions_get_unknown(spark):
    master = build_orders_master(_silver_orders(spark), _silver_customers(spark), _silver_products(spark))
    row = master.filter("order_line_id = 2").first()
    assert (row.customer_name, row.country, row.category, row.sub_category) == (
        "Unknown", "Unknown", "Unknown", "Unknown"
    )


def test_master_preserves_order_count_and_profit(spark):
    master = build_orders_master(_silver_orders(spark), _silver_customers(spark), _silver_products(spark))
    assert master.count() == 2
    assert master.agg(F.sum("profit")).first()[0] == Decimal("48.77")


def test_master_duplicate_dimension_keys_fail(spark):
    dup_customers = _silver_customers(spark).unionByName(
        _silver_customers(spark).withColumn("customer_name", F.lit("Another Name"))
    )
    with pytest.raises(ValueError):
        build_orders_master(_silver_orders(spark), dup_customers, _silver_products(spark))
