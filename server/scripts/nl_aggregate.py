import sys
import os
import json
import re
import time
import pandas as pd

print("DEBUG: argv:", sys.argv, file=sys.stderr)

# Usage: python nl_aggregate.py <json_file> <question>
if len(sys.argv) < 3:
    print(json.dumps({"error": "Usage: python nl_aggregate.py <json_file> <question>"}))
    sys.exit(1)

json_file = sys.argv[1]
question = sys.argv[2]
print("DEBUG: inputs:", {"json_file": json_file, "question": question}, file=sys.stderr)

if not os.path.exists(json_file):
    print(json.dumps({"error": f"File not found: {json_file}"}))
    sys.exit(1)

load_start = time.time()
try:
    with open(json_file, "r", encoding="utf-8") as f:
        doc = json.load(f)
    if (
        "metadata" in doc
        and "structuredData" in doc["metadata"]
        and "data" in doc["metadata"]["structuredData"]
    ):
        data = doc["metadata"]["structuredData"]["data"]
    else:
        print(json.dumps({"error": "No structuredData.data found in JSON file."}))
        sys.exit(1)
    df = pd.DataFrame(data)
    print("DEBUG: load time (s):", time.time() - load_start, file=sys.stderr)
except Exception as e:
    print(json.dumps({"error": f"Failed to load DataFrame: {str(e)}"}))
    sys.exit(1)

# Helpers

def normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).lower())

# Parse pattern
m = re.search(r"average\s+(.+?)\s+for\s+the\s+past\s+(\d+)\s+years?", question, re.IGNORECASE)
if not m:
    print(json.dumps({"error": "Question not supported by deterministic aggregator."}))
    sys.exit(1)

target_phrase = m.group(1).strip()
years_back = int(m.group(2))
print("DEBUG: parsed target and years:", {"target_phrase": target_phrase, "years_back": years_back}, file=sys.stderr)

# Find year column
year_col = None
for c in df.columns:
    if "year" in normalize_name(c):
        year_col = c
        break
if year_col is None:
    for c in df.columns:
        ser = pd.to_numeric(df[c], errors="coerce")
        if ser.notna().sum() == 0:
            continue
        vals = ser.dropna().astype(int)
        if len(vals) == 0:
            continue
        if (vals.between(1900, 2100).mean() > 0.5):
            year_col = c
            break
print("DEBUG: detected year_col:", year_col, file=sys.stderr)

if year_col is None:
    print(json.dumps({"error": "Could not detect a 'year' column."}))
    sys.exit(1)

years_numeric = pd.to_numeric(df[year_col], errors="coerce")
valid_years = years_numeric.dropna().astype(int)
print("DEBUG: number of valid year rows:", len(valid_years), file=sys.stderr)
if len(valid_years) == 0:
    print(json.dumps({"error": "No valid year values found."}))
    sys.exit(1)

recent_years = sorted(valid_years.unique(), reverse=True)[:years_back]
subset = df[years_numeric.isin(recent_years)].copy()
print("DEBUG: recent_years selected:", list(recent_years), file=sys.stderr)
print("DEBUG: subset shape:", subset.shape, file=sys.stderr)

# Choose target column by fuzzy match

def score(col: str) -> int:
    cn = normalize_name(col)
    tp = normalize_name(target_phrase)
    common = set(re.findall(r"[a-z]+", cn)) & set(re.findall(r"[a-z]+", tp))
    bonus = 2 if ("time" in cn and "time" in tp) else 0
    return len(common) + bonus

if subset.shape[0] == 0:
    print(json.dumps({"error": "No rows within the requested year range."}))
    sys.exit(1)

target_col = max(df.columns, key=score)
print("DEBUG: chosen target_col:", target_col, file=sys.stderr)

target_norm = normalize_name(target_col)

try:
    if "time" in target_norm:
        td = pd.to_timedelta(subset[target_col].astype(str), errors="coerce")
        seconds = td.dt.total_seconds()
        avg_seconds = seconds.mean()
        print("DEBUG: time avg seconds:", avg_seconds, file=sys.stderr)
        if pd.isna(avg_seconds):
            raise ValueError("Could not compute average of time column")
        h = int(avg_seconds // 3600)
        m = int((avg_seconds % 3600) // 60)
        s = int(avg_seconds % 60)
        answer_str = f"Average {target_col} over past {years_back} years: {h:01d}:{m:02d}:{s:02d}"
    else:
        series = pd.to_numeric(subset[target_col], errors="coerce")
        avg_val = series.mean()
        print("DEBUG: numeric avg value:", avg_val, file=sys.stderr)
        if pd.isna(avg_val):
            raise ValueError("Could not compute average of numeric column")
        answer_str = f"Average {target_col} over past {years_back} years: {avg_val:.4f}"
    print(json.dumps({"answer": answer_str}))
except Exception as e:
    print(json.dumps({"error": f"Deterministic aggregator error: {str(e)}"}))
    sys.exit(1)
