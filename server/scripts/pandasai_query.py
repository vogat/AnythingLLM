import sys
import os
import json
import re
import time
import pandas as pd
from pandasai import SmartDataframe
from pandasai.llm.ollama import Ollama

print("DEBUG: argv:", sys.argv, file=sys.stderr)

# Usage: python pandasai_query.py <json_file> <question>
if len(sys.argv) < 3:
    print(json.dumps({"error": "Usage: python pandasai_query.py <json_file> <question>"}))
    sys.exit(1)

json_file = sys.argv[1]
question = sys.argv[2]
print("DEBUG: inputs:", {"json_file": json_file, "question": question}, file=sys.stderr)

# Load DataFrame from JSON file (expects structuredData.data)
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

    # 🔁 Auto-detect numeric columns (skip times/dates)
    def looks_like_time_or_date(series: pd.Series) -> bool:
        sample = series.dropna().astype(str).head(10).tolist()
        time_patterns = [
            r"^\d{1,2}:\d{2}$",            # 2:58
            r"^\d{1,2}:\d{2}:\d{2}$",      # 2:58:49
            r"^\d{1,2}/\d{1,2}/\d{2,4}$",  # 12/31/2021
            r"^\d{4}-\d{2}-\d{2}$"         # 2021-12-31
        ]
        for val in sample:
            for pattern in time_patterns:
                if re.match(pattern, val.strip()):
                    return True
        return False

    print("DEBUG: original columns:", list(df.columns), file=sys.stderr)
    conv_count = 0
    for col in df.columns:
        if df[col].dtype == "object" and not looks_like_time_or_date(df[col]):
            try_convert = pd.to_numeric(df[col], errors="coerce")
            if try_convert.notna().sum() > len(df) * 0.5:
                df[col] = try_convert
                conv_count += 1
    print("DEBUG: numeric conversions applied:", conv_count, file=sys.stderr)
except Exception as e:
    print(json.dumps({"error": f"Failed to load DataFrame: {str(e)}"}))
    sys.exit(1)

# Deterministic aggregator: handle patterns like "average <col> for the past N years"
def normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).lower())

try:
    pattern = re.search(r"average\s+(.+?)\s+for\s+the\s+past\s+(\d+)\s+years?", question, re.IGNORECASE)
    if pattern:
        target_phrase = pattern.group(1).strip()
        years_back = int(pattern.group(2))

        # Find year-like column
        year_col = None
        for c in df.columns:
            if "year" in normalize_name(c):
                year_col = c
                break
        if year_col is None:
            # Try to detect a year column by numeric 4-digit values
            for c in df.columns:
                ser = pd.to_numeric(df[c], errors="coerce")
                if ser.notna().sum() > 0:
                    # Heuristic: majority are in plausible year range 1900-2100
                    vals = ser.dropna().astype(int)
                    if (vals.between(1900, 2100).mean() > 0.5):
                        year_col = c
                        break

        if year_col is not None:
            # Select most recent N years present in data
            years_numeric = pd.to_numeric(df[year_col], errors="coerce")
            valid_years = years_numeric.dropna().astype(int)
            if len(valid_years) > 0:
                recent_years = sorted(valid_years.unique(), reverse=True)[:years_back]
                subset = df[years_numeric.isin(recent_years)].copy()

                # Resolve target column by best fuzzy match of target_phrase
                def score(col: str) -> int:
                    cn = normalize_name(col)
                    tp = normalize_name(target_phrase)
                    # simple overlap score
                    common = set(re.findall(r"[a-z]+", cn)) & set(re.findall(r"[a-z]+", tp))
                    bonus = 2 if ("time" in cn and "time" in tp) else 0
                    return len(common) + bonus

                target_col = max(df.columns, key=score)
                target_norm = normalize_name(target_col)

                if "time" in target_norm:
                    # Convert to timedelta then seconds
                    td = pd.to_timedelta(subset[target_col].astype(str), errors="coerce")
                    seconds = td.dt.total_seconds()
                    avg_seconds = seconds.mean()
                    if pd.isna(avg_seconds):
                        raise ValueError("Could not compute average of time column")
                    # Format HH:MM:SS
                    h = int(avg_seconds // 3600)
                    m = int((avg_seconds % 3600) // 60)
                    s = int(avg_seconds % 60)
                    answer_str = f"Average {target_col} over past {years_back} years: {h:01d}:{m:02d}:{s:02d}"
                else:
                    series = pd.to_numeric(subset[target_col], errors="coerce")
                    avg_val = series.mean()
                    if pd.isna(avg_val):
                        raise ValueError("Could not compute average of numeric column")
                    answer_str = f"Average {target_col} over past {years_back} years: {avg_val:.4f}"

                print(json.dumps({"answer": answer_str, "code": None}, ensure_ascii=False))
                sys.exit(0)
except Exception as e:
    # If deterministic path fails, we continue to LLM
    pass

# Set up SmartDataframe with Ollama (configurable model)
model_name = os.environ.get("OLLAMA_MODEL", "llama3:8b")
print("DEBUG: using Ollama model:", model_name, file=sys.stderr)
llm = Ollama(model=model_name)
sdf = SmartDataframe(df, config={"llm": llm, "save_logs": False})

# Improved prompt for robust code generation
wrapped_question = (
    "You are working with a pandas DataFrame named `df`.\n"
    "Never use `dfs`, `dfs[0]`, or any variable except `df`.\n"
    "If you need to filter, use `df[...]` or `df.loc[...]` only.\n"
    "Always return code inside triple backticks.\n"
    "If the answer is a single value, return code that extracts that value from the DataFrame and prints it.\n\n"
    f"User question:\n{question}"
)

try:
    t0 = time.time()
    result = sdf.chat(wrapped_question)
    print("DEBUG: sdf.chat elapsed (s):", time.time() - t0, file=sys.stderr)
    code = None
    if isinstance(result, dict) and "code" in result:
        code = result["code"]
    elif hasattr(sdf, "last_code_executed"):
        code = sdf.last_code_executed
    output = {
        "answer": result.get("answer") if isinstance(result, dict) else result,
        "code": code
    }
    print(json.dumps(output, ensure_ascii=False))
except Exception as e:
    code = getattr(sdf, "last_code_executed", None)
    print("DEBUG: PandasAI error:", str(e), file=sys.stderr)
    print(json.dumps({"error": f"PandasAI error: {str(e)}", "code": code}))
    sys.exit(1)
