#!/usr/bin/env python3

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import yaml


@dataclass
class ValidationError:
    row_index: Optional[int]  # None for dataset-level errors
    field: Optional[str]      # None for dataset-level errors
    code: str
    message: str
    value: Any

    def to_dict(self) -> Dict[str, Any]:
        return {
            "row_index": self.row_index,
            "field": self.field,
            "code": self.code,
            "message": self.message,
            "value": self.value,
        }


def load_schema(schema_path: str) -> Dict[str, Any]:
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = yaml.safe_load(f)
    if not isinstance(schema, dict):
        raise ValueError("Schema root must be a mapping/dictionary")

    fields = schema.get("fields")
    if not isinstance(fields, list) or not fields:
        raise ValueError("Schema must contain a non-empty 'fields' list")

    # Normalize by field name for fast access
    field_map: Dict[str, Dict[str, Any]] = {}
    for field in fields:
        if not isinstance(field, dict) or "name" not in field or "type" not in field:
            raise ValueError("Each field must be a mapping with at least 'name' and 'type'")
        field_map[field["name"]] = field

    schema["_field_map"] = field_map
    return schema


def load_data(data_path: str, delimiter: Optional[str] = None, encoding: str = "utf-8") -> pd.DataFrame:
    if data_path.lower().endswith(".csv"):
        return pd.read_csv(data_path, delimiter=delimiter, encoding=encoding, dtype=str, keep_default_na=False, na_values=["", "NA", "NaN", "null", "None"])
    if data_path.lower().endswith(".json"):
        with open(data_path, "r", encoding=encoding) as f:
            data = json.load(f)
        if isinstance(data, list):
            return pd.DataFrame(data)
        if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
            return pd.DataFrame(data["data"])
        raise ValueError("JSON must be an array of objects or an object with a 'data' array")
    raise ValueError("Unsupported file type. Only .csv and .json are supported")


def is_null(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def cast_value(value: Any, type_name: str, fmt: Optional[str] = None) -> Tuple[Optional[Any], Optional[str]]:
    if is_null(value):
        return None, None

    try:
        if type_name == "string":
            return str(value), None
        if type_name == "integer":
            if isinstance(value, bool):
                return None, "bool_is_not_integer"
            return int(str(value).strip()), None
        if type_name == "float":
            if isinstance(value, bool):
                return None, "bool_is_not_float"
            return float(str(value).strip()), None
        if type_name == "boolean":
            text = str(value).strip().lower()
            if text in {"true", "1", "yes", "y"}:
                return True, None
            if text in {"false", "0", "no", "n"}:
                return False, None
            return None, "invalid_boolean"
        if type_name == "date":
            if not fmt:
                fmt = "%Y-%m-%d"
            return datetime.strptime(str(value).strip(), fmt).date(), None
        if type_name == "datetime":
            if not fmt:
                fmt = "%Y-%m-%d %H:%M:%S"
            return datetime.strptime(str(value).strip(), fmt), None
    except Exception as exc:  # noqa: BLE001
        return None, f"cast_error:{type(exc).__name__}"

    return None, "unknown_type"


def validate_row(row_index: int, row: Dict[str, Any], schema: Dict[str, Any]) -> List[ValidationError]:
    errors: List[ValidationError] = []
    field_map: Dict[str, Dict[str, Any]] = schema["_field_map"]

    for field_name, field_spec in field_map.items():
        raw_value = row.get(field_name)
        required = bool(field_spec.get("required", False))

        if required and is_null(raw_value):
            errors.append(ValidationError(row_index, field_name, "required", "Field is required", raw_value))
            continue

        value, cast_err = cast_value(raw_value, field_spec.get("type"), field_spec.get("format"))

        if cast_err is not None:
            if not is_null(raw_value):
                errors.append(ValidationError(row_index, field_name, "type", f"Value does not match type {field_spec.get('type')}", raw_value))
            continue

        if is_null(value):
            # nothing else to validate
            continue

        # Numeric bounds
        if field_spec.get("type") in {"integer", "float"}:
            if "min" in field_spec:
                try:
                    min_val = float(field_spec["min"])
                    if float(value) < min_val:
                        errors.append(ValidationError(row_index, field_name, "min", f"Value {value} < min {min_val}", value))
                except Exception:  # noqa: BLE001
                    errors.append(ValidationError(row_index, field_name, "schema_error", "Invalid 'min' in schema", field_spec.get("min")))
            if "max" in field_spec:
                try:
                    max_val = float(field_spec["max"])
                    if float(value) > max_val:
                        errors.append(ValidationError(row_index, field_name, "max", f"Value {value} > max {max_val}", value))
                except Exception:  # noqa: BLE001
                    errors.append(ValidationError(row_index, field_name, "schema_error", "Invalid 'max' in schema", field_spec.get("max")))

        # String length bounds
        if field_spec.get("type") == "string":
            if "min_length" in field_spec:
                try:
                    min_len = int(field_spec["min_length"])
                    if len(str(value)) < min_len:
                        errors.append(ValidationError(row_index, field_name, "min_length", f"Length {len(str(value))} < min_length {min_len}", value))
                except Exception:  # noqa: BLE001
                    errors.append(ValidationError(row_index, field_name, "schema_error", "Invalid 'min_length' in schema", field_spec.get("min_length")))
            if "max_length" in field_spec:
                try:
                    max_len = int(field_spec["max_length"])
                    if len(str(value)) > max_len:
                        errors.append(ValidationError(row_index, field_name, "max_length", f"Length {len(str(value))} > max_length {max_len}", value))
                except Exception:  # noqa: BLE001
                    errors.append(ValidationError(row_index, field_name, "schema_error", "Invalid 'max_length' in schema", field_spec.get("max_length")))

        # Regex constraint
        if "regex" in field_spec and not is_null(value):
            try:
                pattern = re.compile(str(field_spec["regex"]))
                if not pattern.fullmatch(str(value)):
                    errors.append(ValidationError(row_index, field_name, "regex", "Value does not match required pattern", value))
            except re.error as re_err:
                errors.append(ValidationError(row_index, field_name, "schema_error", f"Invalid regex in schema: {re_err}", field_spec.get("regex")))

        # Enum constraint
        if "enum" in field_spec:
            allowed = field_spec["enum"]
            if isinstance(allowed, list):
                if str(value) not in {str(x) for x in allowed}:
                    errors.append(ValidationError(row_index, field_name, "enum", f"Value not in allowed set {allowed}", value))
            else:
                errors.append(ValidationError(row_index, field_name, "schema_error", "'enum' must be a list", allowed))

    return errors


def validate_uniques(df: pd.DataFrame, schema: Dict[str, Any]) -> List[ValidationError]:
    errors: List[ValidationError] = []
    field_map: Dict[str, Dict[str, Any]] = schema["_field_map"]

    # Field-level unique
    for field_name, field_spec in field_map.items():
        if field_spec.get("unique"):
            if field_name not in df.columns:
                errors.append(ValidationError(None, field_name, "missing_column", "Column missing for unique check", None))
                continue
            duplicates = df[df[field_name].astype(str).duplicated(keep=False)]
            for idx, val in zip(duplicates.index.tolist(), duplicates[field_name].tolist()):
                errors.append(ValidationError(int(idx), field_name, "unique", "Duplicate value for unique field", val))

    # Dataset-level unique combinations
    dataset = schema.get("dataset", {}) or {}
    unique_combos = dataset.get("unique", []) or []
    for combo in unique_combos:
        if not isinstance(combo, list) or not combo:
            errors.append(ValidationError(None, None, "schema_error", "Each dataset.unique entry must be a non-empty list of column names", combo))
            continue
        missing = [c for c in combo if c not in df.columns]
        if missing:
            errors.append(ValidationError(None, None, "missing_column", f"Columns missing for unique combo: {missing}", None))
            continue
        key_series = df[combo].astype(str).agg("||".join, axis=1)
        duplicates = df[key_series.duplicated(keep=False)]
        for idx, row_vals in zip(duplicates.index.tolist(), duplicates[combo].astype(str).agg("||".join, axis=1).tolist()):
            errors.append(ValidationError(int(idx), ",".join(combo), "unique_combo", "Duplicate combination of values", row_vals))

    return errors


def validate_references(df: pd.DataFrame, schema: Dict[str, Any]) -> List[ValidationError]:
    errors: List[ValidationError] = []
    refs = schema.get("references", []) or []
    for ref in refs:
        field_name = ref.get("field")
        ref_file = ref.get("reference_file")
        ref_field = ref.get("reference_field")
        if not field_name or not ref_file or not ref_field:
            errors.append(ValidationError(None, field_name, "schema_error", "reference requires 'field', 'reference_file', 'reference_field'", ref))
            continue
        try:
            ref_df = load_data(ref_file)
        except Exception as exc:  # noqa: BLE001
            errors.append(ValidationError(None, field_name, "reference_load_error", f"Failed to load reference file: {exc}", ref_file))
            continue
        if ref_field not in ref_df.columns:
            errors.append(ValidationError(None, field_name, "reference_missing_column", f"Reference column '{ref_field}' missing", ref_field))
            continue
        allowed = set(ref_df[ref_field].astype(str).tolist())
        if field_name not in df.columns:
            errors.append(ValidationError(None, field_name, "missing_column", "Column missing for reference check", None))
            continue
        for idx, val in zip(df.index.tolist(), df[field_name].astype(str).tolist()):
            if is_null(val):
                continue
            if val not in allowed:
                errors.append(ValidationError(int(idx), field_name, "foreign_key", f"Value not found in reference '{ref_file}:{ref_field}'", val))

    return errors


def validate(df: pd.DataFrame, schema: Dict[str, Any]) -> List[ValidationError]:
    errors: List[ValidationError] = []

    # Row-level validations
    for idx, row in df.fillna("").to_dict(orient="index").items():
        errors.extend(validate_row(int(idx), row, schema))

    # Dataset-level validations
    errors.extend(validate_uniques(df, schema))
    errors.extend(validate_references(df, schema))

    return errors


def summarize_errors(errors: List[ValidationError]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {"total_errors": len(errors), "by_code": {}, "by_field": {}}
    for err in errors:
        summary["by_code"].setdefault(err.code, 0)
        summary["by_code"][err.code] += 1
        field_key = err.field or "__dataset__"
        summary["by_field"].setdefault(field_key, 0)
        summary["by_field"][field_key] += 1
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate CSV/JSON data against a YAML schema")
    parser.add_argument("--data", required=True, help="Path to input data file (.csv or .json)")
    parser.add_argument("--schema", required=True, help="Path to YAML schema file")
    parser.add_argument("--out", default=None, help="Optional path to write full validation report as JSON")
    parser.add_argument("--delimiter", default=None, help="CSV delimiter (defaults to auto)")
    parser.add_argument("--encoding", default="utf-8", help="File encoding (default: utf-8)")
    parser.add_argument("--max-print", type=int, default=20, help="Max number of errors to print to stdout (default: 20)")
    parser.add_argument("--no-error-exit", action="store_true", help="Do not exit with non-zero code when errors are found")

    args = parser.parse_args()

    try:
        schema = load_schema(args.schema)
        df = load_data(args.data, delimiter=args.delimiter, encoding=args.encoding)
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to load inputs: {exc}", file=sys.stderr)
        return 2

    errors = validate(df, schema)
    summary = summarize_errors(errors)

    print("Validation summary:")
    print(json.dumps(summary, indent=2, ensure_ascii=False))

    if errors:
        print(f"\nFirst {min(len(errors), args.max_print)} errors:")
        for err in errors[: args.max_print]:
            print(json.dumps(err.to_dict(), ensure_ascii=False))

    if args.out:
        report = {
            "summary": summary,
            "errors": [e.to_dict() for e in errors],
        }
        try:
            with open(args.out, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            print(f"\nFull report written to {args.out}")
        except Exception as exc:  # noqa: BLE001
            print(f"Failed to write report: {exc}", file=sys.stderr)

    if errors and not args.no_error_exit:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())