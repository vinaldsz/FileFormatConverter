# FileFormatConverter

FileFormatConverter is a small Python toolkit that converts partitioned CSV files (example: Retail DB sample) into newline-delimited JSON (NDJSON) using schema-driven column mapping. The project demonstrates a practical, minimal ETL step: load schema metadata, apply ordered column names, and convert per-partition files into JSON records.

## What this does

- Loads column definitions from `data/retail_db/schemas.json`.
- Reads source partition files under `data/retail_db/<table>/part-*` (CSV, comma-separated with quoted fields).
- Writes newline-delimited JSON files under `data/retail_db_json/<table>/`.

## Requirements

- Python 3.8+
- pandas

Install dependencies (recommended inside a virtual environment):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

- Set environment variables (optional). The code reads these env vars in `process_all_tables()` if you prefer not to pass explicit paths:

```bash
export SRC_BASE_DIR="data/retail_db"
export TGT_BASE_PATH="data/retail_db_json"
```

- Run the main script to process all tables listed in the schema:

```bash
python app.py
```

- Or process a subset of tables by passing a JSON array of table names as the first argument:

```bash
python app.py '[\"customers\",\"products\"]'
```

## Notebooks

- `Get Column Names.ipynb` shows how the schema is read and how column names are obtained for a table.

## License

This project uses the repository license in `LICENSE`.

---
