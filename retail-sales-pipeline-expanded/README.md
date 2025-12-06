# Retail Sales Data Pipeline (Mini Project)

**Author:** Shivam Singh
**Guide:** Dr. G. Babu
**Degree:** MCA

This repository contains code, scripts, and test utilities for a cloud-native Retail Sales Data Pipeline built on Azure Databricks, ADLS Gen2 and Delta Lake following the Medallion (Bronze/Silver/Gold) architecture.

## Contents
- `notebooks/` - Databricks-ready Python scripts (PySpark) for each module
- `scripts/` - CLI wrapper scripts to run modules locally (for dev/testing with pyspark)
- `tests/` - Simple unit/integration tests using `pytest` (local-friendly)
- `adf/` - Sample Azure Data Factory pipeline JSON (template)
- `infra/` - Deployment hints and ARM template placeholders
- `docs/` - Diagrams, architecture notes, and Power BI guidance

## How to use (local simulation)
This repo provides PySpark scripts that can be run in Databricks. To run locally for testing, you need a PySpark environment.
1. Install dependencies: `pip install -r requirements.txt`
2. Use `scripts/run_module.sh` to execute modules (requires `pyspark` installed).
3. Manifest & metrics are written to `./local_output/` when running locally.

## What is included
- Bronze ingestion (auto-loader style simulation)
- Silver transformation (cleaning, dedupe, DQ checks)
- Gold aggregation & optimization steps (OPTIMIZE simulated)
- Manifest generation and validation scripts
- ADF pipeline skeleton and job orchestration notes
- Unit tests for transformation logic

> Note: In production, run these scripts as Databricks notebooks or jobs and replace local paths with `/mnt/adls/...` mount points and Databricks-specific configs.

