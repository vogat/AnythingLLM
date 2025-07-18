const express = require("express");
const { PythonShell } = require("python-shell");
const path = require("path");
const fs = require("fs");

const router = express.Router();

// POST /query-csv
// Body: { documentId: string, question: string, tableName?: string, useRowHeaders?: boolean }
router.post("/query-csv", async (req, res) => {
  const { documentId, question, tableName = "csvtable", useRowHeaders = false } = req.body;

  if (!documentId || !question) {
    return res.status(400).json({
      success: false,
      error: "Missing documentId or question.",
    });
  }

  const scriptPath = path.join(__dirname, "../../scripts/query_csv.py");
  const tableMap = {};
  tableMap[tableName] = documentId;

  const options = {
    mode: "text",
    pythonOptions: ["-u"],
    scriptPath: path.dirname(scriptPath),
    args: [JSON.stringify(tableMap), question, useRowHeaders ? "true" : "false"],
  };

  try {
    PythonShell.run(path.basename(scriptPath), options, (err, results) => {
      if (err) {
        console.error("PythonShell error:", err);
        return res.status(500).json({ success: false, error: err.message });
      }
      if (!results || !results.length) {
        return res.status(500).json({
          success: false,
          error: "No result returned from Python script.",
        });
      }
      try {
        const result = JSON.parse(results.join(""));
        return res.status(200).json({
          success: true,
          response: result,
        });
      } catch (e) {
        return res.status(500).json({
          success: false,
          error: "Failed to parse Python output.",
        });
      }
    });
  } catch (e) {
    console.error("Unexpected error in CSV query endpoint:", e);
    res.status(500).json({ success: false, error: e.message });
  }
});

module.exports = router;
