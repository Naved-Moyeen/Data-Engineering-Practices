"""Tests for Gold-layer aggregation."""
from decimal import Decimal

from pyspark.sql import Row
from pyspark.sql import functions as F

from src.gold import aggregate_profit
from src.silver import build_orders_master


def _customers(spark):
    return spark.createDataFrame([
        Row(customer_id="C1", customer_name="Alice", country="United States"),
        Row(customer_id="C2", customer_name="Bob", country="United States"),
    ])


def _products(spark):
    return spark.createDataFrame([
        Row(product_id="P1", category="Furniture", sub_category="Chairs"),
        Row(product_id="P2", category="Technology", sub_category="Phones"),
    ])


def _orders(spark):
    return spark.createDataFrame([
        Row(order_line_id=1, order_id="O1", order_date="2016-08-21", order_year=2016,
            ship_mode="Standard Class", customer_id="C1", product_id="P1",
            quantity=1, price=Decimal("100.00"), discount=Decimal("0.00"), profit=Decimal("10.00")),
        Row(order_line_id=2, order_id="O2", order_date="2016-09-15", order_year=2016,
            ship_mode="Standard Class", customer_id="C2", product_id="P2",
            quantity=1, price=Decimal("50.00"), discount=Decimal("0.00"), profit=Decimal("5.00")),
        Row(order_line_id=3, order_id="O3", order_date="2017-01-05", order_year=2017,
            ship_mode="Standard Class", customer_id="C1", product_id="P2",
            quantity=1, price=Decimal("75.00"), discount=Decimal("0.00"), profit=Decimal("7.50")),
    ])


def _master(spark):
    return build_orders_master(_orders(spark), _customers(spark), _products(spark))


def test_gold_aggregates_at_the_required_grain(spark):
    gold = aggregate_profit(_master(spark))
    expected = {"year", "category", "sub_category", "customer_id", "customer_name", "profit"}
    assert set(gold.columns) == expected
    assert gold.count() == 3
    actual = {
        (row.year, row.category, row.sub_category, row.customer_id): row.profit
        for row in gold.collect()
    }
    assert actual == {
        (2016, "Furniture", "Chairs", "C1"): Decimal("10.00"),
        (2016, "Technology", "Phones", "C2"): Decimal("5.00"),
        (2017, "Technology", "Phones", "C1"): Decimal("7.50"),
    }


def test_cross_level_profit_reconciles(spark):
    master = _master(spark)
    gold = aggregate_profit(master)

    by_year = master.groupBy("order_year").agg(F.sum("profit").alias("p")).agg(F.sum("p")).first()[0]
    by_year_cat = master.groupBy("order_year", "category").agg(F.sum("profit").alias("p")).agg(F.sum("p")).first()[0]
    master_total = master.agg(F.sum("profit")).first()[0]
    gold_total = gold.agg(F.sum("profit")).first()[0]

    assert by_year == by_year_cat == master_total == gold_total == Decimal("22.50")


def test_price_reconciles_with_quantity_times_unit_price(spark):
    orders = spark.createDataFrame([
        Row(order_line_id=1, customer_id="C1", product_id="P1",
            quantity=7, price=Decimal("573.17")),
    ])
    products = spark.createDataFrame([
        Row(product_id="P1", category="Furniture", sub_category="Chairs",
            unit_price=Decimal("81.882")),
    ])
    row = (orders.join(products, "product_id")
           .withColumn("expected_price", F.col("quantity") * F.col("unit_price"))
           .withColumn("price_diff", F.abs(F.col("price") - F.col("expected_price")))
           .first())
    assert row["price_diff"] <= Decimal("0.01")
