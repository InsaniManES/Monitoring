# Rami Levy Product Database

This repository now includes a database builder that saves all available Rami Levy products into SQLite.

## Data source

The script uses the public transparency portal:

- `https://url.publishedprices.co.il`
- Username: `RamiLevi`
- Chain ID: `7290058140886`

It logs in, finds `PriceFull` files, picks the newest file per branch, parses product XML, and upserts rows into a local database.

## Run

```bash
python3 build_rami_levy_database.py
```

Optional flags:

```bash
# Write to a custom DB path
python3 build_rami_levy_database.py --db-path data/rami_levy_products.db

# Ingest only first 3 branches (smoke test)
python3 build_rami_levy_database.py --max-branches 3

# Enable certificate verification (off by default in this environment)
python3 build_rami_levy_database.py --verify-ssl
```

## Database schema

Main tables:

- `products` - canonical product metadata (one row per `item_code`)
- `branches` - latest ingested file per branch
- `branch_product_prices` - latest product price per branch
- `ingestion_runs` - run history and status

## Notes

- The importer handles both gzip and zip payloads.
- The script is idempotent: reruns update existing rows with latest values.
