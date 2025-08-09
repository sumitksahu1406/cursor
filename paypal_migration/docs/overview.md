## Migration Overview

This project illustrates converting a typical SQL/Scala Spark pipeline to Python (PySpark) while preserving semantics:

- Deterministic deduplication with window functions
- Currency normalization with forward-filled FX rates
- Reference data joins (merchants, users)
- Business-ready aggregates (merchant daily metrics)

You can adapt the schemas and transformations to your actual PayPal domain models.