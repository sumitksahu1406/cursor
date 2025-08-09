# PayPal Data Processing Migration (SQL/Scala to PySpark)

This project demonstrates an end-to-end migration of a typical PayPal-style data processing flow from SQL/Scala to Python (PySpark). It implements a simple lakehouse-style pipeline:

- Bronze: Raw ingestion from CSV into Parquet with audit columns
- Silver: Cleansing, deduplication, type casting, currency normalization (to USD), reference data joins
- Gold: Business aggregates (daily merchant metrics: GMV, conversion rate, refund and chargeback rates)

## Run locally

1. Create and activate a virtual environment (optional).
2. Install dependencies:

```
pip install -r requirements.txt
```

3. Execute the pipeline end-to-end:

```
python -m src.pipelines.run_all
```

Artifacts are written to `data/bronze`, `data/silver`, and `data/gold`.

## Tests

```
pytest -q
```

## Layout

```
src/
  pipelines/
    bronze_ingest.py
    silver_transform.py
    gold_aggregates.py
    run_all.py
  utils/
    spark.py
    io.py
    quality.py
```

Input sample data is in `data/input/`. Adjust or replace with your datasets.