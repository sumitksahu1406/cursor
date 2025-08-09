# Data Validation CLI (Python)

Validate CSV/JSON files against a simple YAML schema.

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

```bash
python validate_data.py --data sample.csv --schema schema_example.yml --out report.json
```

Options:
- `--data`: Path to `.csv` or `.json` file
- `--schema`: Path to YAML schema (see `schema_example.yml`)
- `--out`: Optional JSON report file path
- `--delimiter`: CSV delimiter override
- `--encoding`: File encoding (default `utf-8`)
- `--max-print`: How many errors to print to stdout
- `--no-error-exit`: Do not exit with non-zero code when errors exist

## Schema format

```yaml
fields:
  - name: id
    type: integer            # string | integer | float | boolean | date | datetime
    required: true
    unique: true
  - name: email
    type: string
    regex: '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'
  - name: age
    type: integer
    min: 0
    max: 120
  - name: signup_date
    type: date
    format: '%Y-%m-%d'

# Dataset-level unique constraints (composite keys)
dataset:
  unique:
    - [email, signup_date]

# Optional foreign key reference
references:
  - field: country_code
    reference_file: countries.csv
    reference_field: code
```

## Exit codes
- `0`: No errors
- `1`: Validation errors found (unless `--no-error-exit` is set)
- `2`: Failed to load inputs