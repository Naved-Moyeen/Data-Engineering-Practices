"""Source file locations and Bronze table settings."""
from pathlib import Path

BRONZE_SCHEMA = "main.sales_bronze"
DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"

BRONZE_CONFIG = [
    {
        "source_name": "customer",
        "source_format": "excel",
        "source_path": str(DATA_DIR / "Customer.xlsx"),
        "target_table": f"{BRONZE_SCHEMA}.customer",
        "enabled": True,
        "options": {"headerRows": "1", "inferSchema": "false"},
    },
    {
        "source_name": "products",
        "source_format": "csv",
        "source_path": str(DATA_DIR / "Products.csv"),
        "target_table": f"{BRONZE_SCHEMA}.products",
        "enabled": True,
        "options": {
            "header": "true",
            "inferSchema": "false",
            "encoding": "UTF-8",
            "escape": '"',
        },
    },
    {
        "source_name": "orders",
        "source_format": "json",
        "source_path": str(DATA_DIR / "Orders.json"),
        "target_table": f"{BRONZE_SCHEMA}.orders",
        "enabled": True,
        "options": {"multiLine": "true"},
    },
]


def get_enabled_sources():
    return [source for source in BRONZE_CONFIG if source.get("enabled", False)]


def get_source_config(source_name: str):
    for source in BRONZE_CONFIG:
        if source["source_name"].casefold() == source_name.casefold():
            return source
    raise ValueError(f"No source configuration found for {source_name!r}")
