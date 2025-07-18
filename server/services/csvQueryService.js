const { spawn } = require("child_process");
const path = require("path");
const { loadedCSVDataframes } = require("../utils/files/csvImportService");

/**
 * Process a question against a loaded CSV using DuckDB Python script.
 * @param {string} documentId - The ID of the CSV document
 * @param {string} sqlQuery - The SQL query string
 * @param {string} [tableName] - The table name to use (default: 'csvtable')
 * @param {boolean} [useRowHeaders] - Whether to use row headers (default: false)
 * @returns {Promise<{success: boolean, data?: any, error?: string}>}
 */
async function queryCSV(documentId, sqlQuery, tableName = "csvtable", useRowHeaders = false) {
  return new Promise((resolve) => {
    const csvPath = loadedCSVDataframes.get(documentId);
    if (!csvPath) {
      console.log('[DEBUG][csvQueryService] CSV not loaded or found for documentId:', documentId);
      return resolve({ success: false, error: "CSV not loaded or found." });
    }
    // The docId is the documentId, and tableName is the SQL table name
    const tableMap = {};
    tableMap[tableName] = documentId;
    const scriptPath = path.resolve(__dirname, "../scripts/query_csv.py");
    console.log('[DEBUG][csvQueryService] About to spawn Python query_csv.py with:', { tableMap, sqlQuery, useRowHeaders });
    const pyProcess = spawn("python", [
      scriptPath,
      JSON.stringify(tableMap),
      sqlQuery,
      useRowHeaders ? "true" : "false"
    ]);
    let output = "";
    let error = "";
    pyProcess.stdout.on("data", (data) => (output += data.toString()));
    pyProcess.stderr.on("data", (data) => {
      error += data.toString();
      // Print all stderr for debugging
      console.log('[DEBUG][csvQueryService][stderr]', data.toString());
    });
    pyProcess.on("close", (code) => {
      let parsed = null;
      try {
        parsed = JSON.parse(output);
      } catch (e) {
        console.log('[DEBUG][csvQueryService] Failed to parse Python output:', output);
        return resolve({ success: false, error: "Failed to parse Python output." });
      }
      if (code !== 0) {
        console.log('[DEBUG][csvQueryService] Python exited with code', code, 'stderr:', error);
        return resolve({ success: false, error: error || "Python script error." });
      }
      console.log('[DEBUG][csvQueryService] Python output parsed:', parsed);
      resolve({ success: true, result: parsed });
    });
  });
}

module.exports = {
  queryCSV
};
