const { spawn } = require("child_process");
const path = require("path");

function queryCsvWithDuckDB(documentId, sqlQuery, tableName = "csvtable", useRowHeaders = false) {
  return new Promise((resolve) => {
    // The docId is the documentId, and tableName is the SQL table name
    const tableMap = {};
    tableMap[tableName] = documentId;
    const scriptPath = path.resolve(__dirname, "../../scripts/query_csv.py");
    const py = spawn("python", [
      scriptPath,
      JSON.stringify(tableMap),
      sqlQuery,
      useRowHeaders ? "true" : "false"
    ]);
    let output = "";
    let error = "";
    py.stdout.on("data", (data) => (output += data.toString()));
    py.stderr.on("data", (data) => (error += data.toString()));
    py.on("close", () => {
      if (error) {
        return resolve({ success: false, error });
      }
      try {
        const parsed = JSON.parse(output);
        resolve({ success: true, result: parsed });
      } catch (e) {
        resolve({ success: false, error: "Failed to parse Python output." });
      }
    });
  });
}

module.exports = { queryCsvWithDuckDB };
