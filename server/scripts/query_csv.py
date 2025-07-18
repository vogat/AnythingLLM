import sys
import pandas as pd
import json
import os
import duckdb
import time
import logging

# Setup logging
logging.basicConfig(filename='query_csv.log', level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Argument 1: JSON string mapping table names to doc IDs
# Argument 2: SQL query string
# Argument 3: use_row_headers ("true" or "false")

table_map = json.loads(sys.argv[1])
question = sys.argv[2]
use_row_headers = sys.argv[3].lower() == "true" if len(sys.argv) > 3 else False

data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../storage/documents/custom-documents"))

tables = {}
schema_info = {}
for table_name, doc_id in table_map.items():
    json_path = None
    print("DEBUG: Files in data_dir:", os.listdir(data_dir), file=sys.stderr)
    print("DEBUG: Looking for doc_id:", doc_id, "or table_name:", table_name, file=sys.stderr)
    for fname in os.listdir(data_dir):
        # Match if filename equals doc_id, or equals doc_id + '.csv', or startswith doc_id
        if fname == str(doc_id) or fname == str(doc_id) + '.csv' or fname.startswith(str(doc_id)) or fname == table_name or fname == table_name + '.csv':
            json_path = os.path.join(data_dir, fname)
            break
    if not json_path or not os.path.exists(json_path):
        print(f"DEBUG: Could not find file for doc_id: {doc_id} in {data_dir}", file=sys.stderr)
        print(f"DEBUG: Files present: {os.listdir(data_dir)}", file=sys.stderr)
        error_msg = f"Document not found for table '{table_name}' (doc_id: {doc_id})"
        logging.error(error_msg)
        print(json.dumps({"error": error_msg}))
        sys.exit()
    if json_path:
        print("DEBUG: Found file at:", json_path, file=sys.stderr)
        with open(json_path, "r", encoding="utf-8") as f:
            preview = f.read(500)
            print("DEBUG: File preview (first 500 chars):", preview, file=sys.stderr)
            f.seek(0)
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            doc = json.load(f)
        # Use structuredData.data if present
        data = None
        if (
            "metadata" in doc
            and "structuredData" in doc["metadata"]
            and "data" in doc["metadata"]["structuredData"]
        ):
            data = doc["metadata"]["structuredData"]["data"]
            print("DEBUG: Using structuredData.data from JSON file", file=sys.stderr)
        else:
            # Fallback: try to parse pageContent as lines (not recommended)
            data = []
            page_content = doc.get("pageContent", "")
            for line in page_content.splitlines():
                if line.strip().startswith("Row "):
                    # Attempt to parse the line into a dict (very basic)
                    row = {}
                    for part in line.split(", "):
                        if ": " in part:
                            k, v = part.split(": ", 1)
                            row[k.strip()] = v.strip()
                    if row:
                        data.append(row)
            print("DEBUG: Used fallback pageContent parsing", file=sys.stderr)
        df = pd.DataFrame(data)
        # Column name normalization
        df.columns = [str(col).strip().lower().replace(' ', '_') for col in df.columns]
        print("DEBUG: DataFrame columns:", df.columns.tolist(), file=sys.stderr)
        print("DEBUG: DataFrame head:\n", df.head(), file=sys.stderr)
        print("DEBUG: DataFrame tail:\n", df.tail(), file=sys.stderr)
        print("DEBUG: DataFrame shape:", df.shape, file=sys.stderr)
        tables[table_name] = df
        schema_info[table_name] = list(df.columns)
    except Exception as e:
        error_msg = f"Failed to load or parse data for table '{table_name}': {str(e)}"
        logging.error(error_msg)
        print(json.dumps({"error": error_msg}))
        sys.exit()

# Execute the question via DuckDB safely
try:
    con = duckdb.connect()
    for table_name, df in tables.items():
        con.register(table_name, df)
    print("DEBUG: SQL query to execute:", question, file=sys.stderr)
    start_time = time.time()
    result = con.execute(question).fetchdf()
    elapsed = time.time() - start_time
    answer = result.to_dict(orient="records")
    metadata = {
        "row_count": len(result),
        "column_count": len(result.columns),
        "columns": list(result.columns),
        "execution_time_seconds": elapsed
    }
    output = {"answer": answer, "metadata": metadata}
    print(json.dumps(output))
    logging.info(f"Query succeeded: {question}")
    print("DEBUG: SQL query result head:\n", result.head(), file=sys.stderr)
    print("DEBUG: SQL query result shape:", result.shape, file=sys.stderr)
    con.close()
except Exception as e:
    import traceback
    traceback.print_exc(file=sys.stderr)
    # User guidance: suggest available tables/columns
    guidance = {
        "available_tables": list(schema_info.keys()),
        "table_columns": schema_info
    }
    error_msg = f"Query failed: {str(e)}"
    logging.error(f"{error_msg} | Query: {question}")
    print(json.dumps({"error": error_msg, "guidance": guidance}))
