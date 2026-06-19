# Netflix ETL Pipeline (PySpark)

A local PySpark ETL pipeline built on the [Netflix Shows dataset](https://www.kaggle.com/datasets/shivamb/netflix-shows) from Kaggle, structured to mirror the extract-transform-validate-load pattern used by managed ETL services like AWS Glue.

## Overview

This project takes raw Netflix metadata, cleans and standardizes it, and produces three aggregated summary tables: content type breakdown, top countries by content volume, and content count by release year. Built primarily to internalize Glue's ETL pattern hands-on before working with the platform directly, using AWS Skill Builder's Glue Getting Started course as a starting reference.

## Pipeline Architecture

```
Raw CSV ingestion
      |
Null handling (country, rating)
      |
Deduplication
      |
Standardization (type: Movie/TV Show -> MOVIE/TV SHOW)
      |
Data quality validation (release_year format check)
      |
Aggregation layer (type, country, year counts)
      |
Output (CSV via collect-and-write)
```

## Dataset

Netflix Shows dataset, Kaggle (shivamb/netflix-shows). Contains show metadata including title, type, director, cast, country, date added, release year, rating, duration, and genre listings.

## The Data Quality Issue

The most useful part of this build wasn't the pipeline itself, it was a data quality bug surfaced during aggregation.

Grouping by `release_year` produced obviously invalid values: actor names, runtime strings like `"40 min"`, and full date strings like `"August 13, 2020"` appeared alongside genuine years. Root cause: unescaped commas inside free-text fields (likely `cast` or `description`) shifted column alignment for a subset of rows during the original CSV export, a classic real-world ETL failure mode that's easy to miss without explicit validation.

**Fix:** added a regex-based validation filter requiring `release_year` to match exactly four digits before aggregation, with a row count delta logged to quantify how many records were affected.

```python
before_count = df.count()
df = df.filter(col("release_year").rlike("^[0-9]{4}$"))
after_count = df.count()
print(f"Filtered {before_count - after_count} rows with malformed release_year values")
```

This is the kind of validation step that matters more at production scale than it appears to on a small dataset, silent schema drift in free-text-heavy sources is a common real ETL failure point, not a hypothetical one.

## Tech Stack

PySpark, Python, local execution (`local[*]` master)

## A Note on Output Writing

Spark's native Hadoop-based file commit protocol (`FileOutputCommitter`) fails on Windows without a correctly configured `winutils.exe`, a known environment issue, both `.parquet()` and `.csv()` writes through Spark's own writer hit this. The fix used here is converting each small aggregated result to a pandas DataFrame with `.toPandas()`, then writing with pandas' own `.to_csv()`, which sidesteps Spark's Hadoop commit path entirely. A reasonable approach when result sets are summary-sized, not appropriate for full-dataset writes at production scale, where collecting everything to the driver would defeat the purpose of distributed processing.

## What This Is Not

This is a local PySpark simulation of Glue's ETL pattern, not a deployed AWS Glue job. No AWS services, S3, or the Glue Data Catalog are used here. The goal was internalizing the transformation logic before working hands-on with the managed service.

## How to Run

```bash
pip install pyspark pandas
python etl_job.py
```

Outputs three CSV files to `output/`: `type_count.csv`, `country_count.csv`, `year_count.csv`.
