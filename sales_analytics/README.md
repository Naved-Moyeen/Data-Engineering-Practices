# E-commerce Sales Analytics

A Databricks batch pipeline that loads customer, product and order files, cleans and enriches the data with PySpark, and produces profit summaries.

## Pipeline

`Customer.xlsx` / `Products.csv` / `Orders.json` → Bronze → Silver → Gold

| Layer | Tables | Purpose |
|---|---|---|
| Bronze | `main.sales_bronze.customer`, `products`, `orders` | Source snapshots with load metadata |
| Silver | `customers`, `customers_quarantine`, `products`, `products_quarantine`, `orders`, `orders_quarantine`, `orders_master` in `main.sales_silver` | Cleaned records, rejected records and enriched order lines |
| Gold | `main.sales_gold.profit_by_year_category_subcat_customer` | Profit by year, category, subcategory and customer |

## Files

```text
sales_analytics/
├── data/raw/                 # Input datasets
├── notebooks/sales_pipeline.py
├── src/
│   ├── bronze_config.py      # Source paths and reader options
│   ├── bronze.py             # Raw ingestion
│   ├── schemas.py            # Silver and Gold schemas
│   ├── silver.py             # Validation, cleaning and enrichment
│   ├── gold.py               # Profit aggregation
│   └── main.py               # Pipeline execution
├── sql/                      # Four requested SQL summaries
├── tests/
├── README.md
├── design_document.md
└── requirements.txt
```

## Setup

1. Use a Databricks Runtime with Spark and Delta Lake.
2. Ensure the identity running the job can create schemas and tables in catalog `main`.
3. Make the files in `data/raw/` available to the cluster. `bronze_config.py` builds paths relative to the project directory; update `DATA_DIR` if the files are stored elsewhere.
4. Install a Spark Excel data-source provider compatible with the selected runtime. The `excel` reader is not included with standard PySpark.

## Run

Open `notebooks/sales_pipeline.py` as a Databricks notebook and run all cells. The notebook locates the project from its current directory or its parent directory. If your workspace layout differs, add the project root to `sys.path` in the first cell.

The pipeline performs a full refresh: each run overwrites the Bronze, Silver and Gold snapshots. It is intended for batch reruns, not incremental ingestion.

## Tests

Run from the project root in an environment with pytest, Java and a PySpark version compatible with the target Databricks Runtime:

```bash
pip install -r requirements.txt
pytest -q
```

`requirements.txt` installs pytest. Databricks provides PySpark in its runtime. For local testing, install a matching PySpark release separately; do not replace the runtime's Spark packages with an unrelated version. The tests use small in-memory DataFrames and do not read the supplied source files.

## SQL summaries

Run the scripts in `sql/` against the Gold table:

- `profit_by_year.sql`
- `profit_by_year_category.sql`
- `profit_by_customer.sql`
- `profit_by_year_customer.sql`

See [design_document.md](design_document.md) for the transformation rules and assumptions.
