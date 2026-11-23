# Design Document — E-commerce Sales Analytics

## Objective

Process the supplied customer, product and order datasets in Databricks using PySpark. Produce cleaned source tables, an enriched order-line table, a profit aggregate and four SQL summaries.

## Architecture

`Source files → Bronze → Silver → Gold → SQL`

| Layer | Output | Description |
|---|---|---|
| Bronze | `main.sales_bronze.customer`, `products`, `orders` | Raw source snapshots plus `source_file`, `load_id` and `ingestion_ts` |
| Silver | `main.sales_silver.customers`, `products`, `orders` | Standardized and validated records |
| Silver | `customers_quarantine`, `products_quarantine`, `orders_quarantine` | Rejected source records with `_quarantine_reason` |
| Silver | `orders_master` | Clean order lines enriched with customer and product attributes |
| Gold | `main.sales_gold.profit_by_year_category_subcat_customer` | Profit by year, category, subcategory and customer |

`src/main.py` runs the layers. Transformation logic is in `src/bronze.py`, `src/silver.py` and `src/gold.py`.

## Source data

| File | Format | Key |
|---|---|---|
| `Customer.xlsx` | Excel | Customer ID |
| `Products.csv` | CSV | Product ID |
| `Orders.json` | Multi-line JSON | Row ID (order-line ID) |

Source paths are configured in `src/bronze_config.py`. The Excel Spark data-source provider must be installed on the cluster.

## Processing rules

### Bronze

- Read the three configured files and preserve their source columns.
- Add source file, load ID and ingestion timestamp.
- Write Delta tables using full-snapshot overwrite.
- Validate source configuration before starting the load. Targets must be within `main.sales_bronze`.

### Silver — shared rules

- Use declared output schemas from `src/schemas.py`; missing optional columns are added as NULL so table schemas stay consistent.
- Convert blank strings to NULL and trim display values.
- Normalize customer and product keys with `trim` and `uppercase` in both dimensions and orders.
- Parse dates using day-first `d/M/yyyy` format.
- Round monetary values to two decimal places using decimal arithmetic.
- Keep negative profit values; `Orders.Profit` is treated as the source of truth.

### Customer and product dimensions

- Records with missing or blank keys are quarantined.
- A shared business-key deduplication rule is applied to customers (`customer_id`), products (`product_id`) and orders (`order_line_id`).
- Exact duplicate business rows are collapsed to one; ingestion metadata is excluded from the comparison because repeated loads can have different audit values.
- If one normalized key has different business attributes, all distinct versions for that key are quarantined. No arbitrary row is selected.
- The number of exact duplicate copies removed is returned in the pipeline summary.
- Product category and subcategory must be present and non-blank. New non-empty values are accepted rather than being restricted to hardcoded lists. If the business later requires a controlled taxonomy, validate category-subcategory pairs against a maintained reference table.
- Product unit price must be present and numeric.

### Orders

- Required columns include order-line ID, dates, customer and product IDs, quantity, price, discount and profit. Missing source columns stop the load with a clear error.
- Invalid or missing order-line IDs, dates and join keys are quarantined.
- Quantity must be a positive integer.
- Price must be numeric and non-negative.
- Discount must be numeric and between 0 and 1 inclusive.
- Profit must be numeric; negative values are valid.
- Exact duplicate order lines are collapsed and counted. Distinct rows with the same order-line ID are all quarantined.
- A row can have more than one quarantine reason; reason codes are separated by `|`.

### Reconciliation

For each source, the pipeline checks:

`Bronze rows = clean rows + quarantined rows + exact duplicate copies removed`

A mismatch raises an error. Conflicting-key rows remain in quarantine and are included in its row count.

### Enrichment and aggregation

- `orders_master` left-joins clean orders to clean customers and products using normalized keys.
- Missing dimension attributes are set to `Unknown`; the order line is retained.
- Gold sums the already-rounded order-line profit at year, category, subcategory, customer ID and customer name grain.
- The four SQL scripts roll up Gold profit by year, year/category, customer, and customer/year.

## Assumptions

- `Orders.Profit` is authoritative. Profit is not recalculated because cost data is not supplied.
- `Orders.Price` is the extended order-line price; `Products.Price per product` is a unit price.
- `Row ID` identifies an order line. An Order ID may have multiple lines.
- Customer identity is based on normalized customer ID, not customer name.
- Dates are day-first. Discount is a fraction from 0 to 1.
- Profit is rounded at line level before aggregation (round-then-sum).
- Unmatched customer and product references are retained in the master table with `Unknown` attributes.
- The pipeline is a full batch refresh. Incremental loads, CDC, historical dimension tracking and orchestration scheduling are not included.

## Performance and operational notes

- Transformations use native PySpark DataFrame expressions.
- Customer and product dimensions are broadcast for the current dataset size. Revisit this if dimensions grow materially.
- Delta tables are stored in Unity Catalog.
- Full refreshes are simple to rerun but rewrite all tables. Add incremental processing, monitoring and alerting before production use.

## Testing

The pytest suite covers source configuration, metadata, cleaning, duplicate handling, quarantine, enrichment and aggregation. Run `pytest -q` in an environment with a compatible Spark installation. Execute the suite in the target Databricks Runtime before release.
