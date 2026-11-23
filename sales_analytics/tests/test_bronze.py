"""Tests for Bronze configuration and load metadata."""
import pytest
from pyspark.sql import Row

from src.bronze import load_metadata, validate_source_configs
from src.bronze_config import (
    BRONZE_SCHEMA,
    get_enabled_sources,
    get_source_config,
)


def test_config_has_three_enabled_sources():
    enabled = get_enabled_sources()
    assert [c["source_name"] for c in enabled] == ["customer", "products", "orders"]


def test_all_targets_confined_to_bronze_schema():
    for c in get_enabled_sources():
        assert c["target_table"].startswith(BRONZE_SCHEMA + ".")


def test_orders_config_uses_multiline():
    assert get_source_config("orders")["options"].get("multiLine", "").lower() == "true"


def test_products_config_uses_quote_escape():
    assert get_source_config("products")["options"].get("escape") == '"'


def test_csv_and_excel_sources_do_not_infer_schema():
    for name in ["customer", "products"]:
        assert get_source_config(name)["options"].get("inferSchema", "true").lower() == "false"


def test_load_metadata_adds_audit_columns(spark):
    df = spark.createDataFrame([Row(**{"Customer ID": " JK-15370 ", "Country": "United States"})])
    out = load_metadata(df, "/some/path/data/raw/Customer.xlsx", "load-42")
    row = out.first()
    assert row["source_file"] == "Customer.xlsx"
    assert row["load_id"] == "load-42"
    assert row["ingestion_ts"] is not None


def test_load_metadata_leaves_raw_values_untouched(spark):
    df = spark.createDataFrame([Row(**{"Customer ID": " JK-15370 ", "Postal Code": "07601"})])
    row = load_metadata(df, "x/Customer.xlsx", "l1").first()
    assert row["Customer ID"] == " JK-15370 "
    assert row["Postal Code"] == "07601"


def test_load_metadata_preserves_row_count(spark):
    df = spark.createDataFrame([Row(a="1"), Row(a="2"), Row(a="3")])
    assert load_metadata(df, "x.csv", "l1").count() == 3


def test_config_rejects_targets_outside_bronze():
    config = dict(get_enabled_sources()[0])
    config["target_table"] = "main.other.customer"
    with pytest.raises(ValueError):
        validate_source_configs([config])


def test_config_rejects_duplicate_source_names():
    config = get_enabled_sources()[0]
    with pytest.raises(ValueError):
        validate_source_configs([config, dict(config)])
