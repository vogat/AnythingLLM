const express = require("express");
const { PythonShell } = require("python-shell");
const path = require("path");
const fs = require("fs");

const router = express.Router();

function nowMs() {
  return Date.now();
}

function log(...args) {
  console.log("[CSV-QUERY-ENDPOINT]", ...args);
}

// Helper: resolve the JSON file path for a given document identifier (numeric id or docId)
async function resolveJsonPath(documentId) {
  const baseDir = path.resolve(__dirname, "../storage/documents/custom-documents");
  log("resolveJsonPath baseDir:", baseDir);
  const exists = fs.existsSync(baseDir);
  if (!exists) {
    throw new Error(`Documents directory not found: ${baseDir}`);
  }

  // Try DB first
  try {
    const { Document } = require("../models/documents");
    let doc = null;
    if (!isNaN(Number(documentId))) {
      log("resolveJsonPath trying prisma by numeric id:", documentId);
      doc = await Document.get({ id: Number(documentId) });
    }
    if (!doc) {
      log("resolveJsonPath trying prisma by docId string:", String(documentId));
      doc = await Document.get({ docId: String(documentId) });
    }
    if (doc) {
      log("resolveJsonPath prisma hit:", { id: doc.id, docId: doc.docId, filename: doc.filename, docpath: doc.docpath });
      const candidates = [];
      if (doc.docpath) {
        const base = path.basename(doc.docpath);
        candidates.push(base);
        if (base.toLowerCase().endsWith(".csv")) candidates.push(base.replace(/\.csv$/i, ".json"));
      }
      if (doc.filename) {
        candidates.push(doc.filename);
        if (doc.filename.toLowerCase().endsWith(".csv")) candidates.push(doc.filename.replace(/\.csv$/i, ".json"));
      }
      const seen = new Set();
      for (const name of candidates) {
        if (!name || seen.has(name)) continue;
        seen.add(name);
        const p = path.join(baseDir, name);
        log("resolveJsonPath checking candidate:", p);
        if (fs.existsSync(p)) return { fileName: name, absPath: p };
      }
    }
  } catch (e) {
    log("resolveJsonPath DB lookup error (non-fatal):", e.message);
  }

  // Fallback: scan for direct filename, try CSV->JSON twin, or startswith id
  const all = fs.readdirSync(baseDir);
  log("resolveJsonPath directory listing count:", all.length);
  const idStr = String(documentId);
  let tryNames = [idStr, `${idStr}.json`, `${idStr}.csv`];
  if (/\.csv$/i.test(idStr)) tryNames.push(idStr.replace(/\.csv$/i, ".json"));

  for (const name of tryNames) {
    const p = path.join(baseDir, name);
    log("resolveJsonPath tryName:", name, "exists:", fs.existsSync(p));
    if (fs.existsSync(p)) return { fileName: name, absPath: p };
  }
  const starts = all.find((f) => f.toLowerCase().startsWith(idStr.toLowerCase()));
  if (starts) return { fileName: starts, absPath: path.join(baseDir, starts) };

  return null;
}

// Auto-detect if a question is likely SQL
function isLikelySql(text = "") {
  return /^(\s*)(SELECT|WITH|INSERT|UPDATE|DELETE|PRAGMA)\b/i.test(text);
}

// Run a python script and capture stdout/stderr with timing
async function runPython(scriptFullPath, args) {
  const t0 = nowMs();
  log("runPython start:", { script: scriptFullPath, args });
  const shell = new PythonShell(path.basename(scriptFullPath), {
    mode: "text",
    pythonOptions: ["-u"],
    scriptPath: path.dirname(scriptFullPath),
    args,
  });
  let stdout = "";
  let stderr = "";
  return await new Promise((resolve) => {
    shell.on("message", (m) => {
      stdout += String(m);
    });
    shell.on("stderr", (m) => {
      stderr += String(m);
      // Also log streaming stderr for real-time insight
      log("[python-stderr]", m);
    });
    shell.on("error", (err) => {
      log("runPython error event:", err.message);
    });
    shell.on("close", (code) => {
      const dt = nowMs() - t0;
      log("runPython closed:", { code, ms: dt });
      if (stderr) log("runPython stderr collected length:", stderr.length);
      if (stdout) log("runPython stdout collected length:", stdout.length);
      try {
        const parsed = stdout ? JSON.parse(stdout) : null;
        // Only consider non-zero numeric codes as failure; undefined codes treated as success if JSON parsed
        if (typeof code === 'number' && code !== 0) {
          return resolve({ success: false, error: `exit ${code}`, stderr, stdout, parsed });
        }
        resolve({ success: true, parsed, stderr, stdout });
      } catch (e) {
        resolve({ success: false, error: `parse error: ${e.message}`, stderr, stdout });
      }
    });
  });
}

// Shared handler
async function handleCsvQuery({ documentId, question, tableName = "csvtable", useRowHeaders = false, mode }) {
  log("handleCsvQuery called:", { documentId, question, tableName, useRowHeaders, mode });
  if (!documentId || !question) {
    return { success: false, error: "Missing documentId or question." };
  }

  let resolved;
  try {
    resolved = await resolveJsonPath(documentId);
  } catch (e) {
    return { success: false, error: e.message };
  }
  log("handleCsvQuery resolved:", resolved);
  if (!resolved) {
    return { success: false, error: `Document not found for id: ${documentId}` };
  }

  const { fileName, absPath } = resolved;
  const useSql = mode === "sql" || (mode !== "nl" && isLikelySql(question));
  log("handleCsvQuery mode selection:", { useSql, mode });

  if (useSql) {
    const scriptPath = path.join(__dirname, "../scripts/query_csv.py");
    const tableMap = { [tableName]: fileName };
    log("SQL path chosen. Table map:", tableMap);

    const result = await runPython(scriptPath, [JSON.stringify(tableMap), question, useRowHeaders ? "true" : "false"]);
    if (!result.success) return { success: false, error: result.error, stderr: result.stderr };
    return { success: true, response: result.parsed };
  }

  // Deterministic NL aggregator for common patterns (e.g., average X for past N years)
  const avgPattern = /average\s+.+?\s+for\s+the\s+past\s+\d+\s+years?/i;
  if (avgPattern.test(question)) {
    log("Aggregator path chosen for question:", question);
    const aggScriptPath = path.join(__dirname, "../scripts/nl_aggregate.py");
    const aggResult = await runPython(aggScriptPath, [absPath, question]);
    if (aggResult.success && aggResult.parsed && !aggResult.parsed.error) {
      return { success: true, response: aggResult.parsed };
    }
    log("Aggregator failed, falling back to PandasAI:", aggResult.error || aggResult.parsed);
  }

  // PandasAI path
  const pandasScriptPath = path.join(__dirname, "../scripts/pandasai_query.py");
  log("PandasAI path chosen.");
  const pandasResult = await runPython(pandasScriptPath, [absPath, question]);
  if (!pandasResult.success) return { success: false, error: pandasResult.error, stderr: pandasResult.stderr };
  return { success: true, response: pandasResult.parsed };
}

// POST /query-csv
router.post("/query-csv", async (req, res) => {
  const t0 = nowMs();
  log("POST /query-csv body:", req.body);
  const { documentId, question, tableName = "csvtable", useRowHeaders = false, mode } = req.body || {};
  const result = await handleCsvQuery({ documentId, question, tableName, useRowHeaders, mode });
  const status = result.success ? 200 : 400;
  log("POST /query-csv result:", { status, tookMs: nowMs() - t0, success: result.success, error: result.error });
  return res.status(status).json(result);
});

// GET /query-csv (for quick tests/health)
router.get("/query-csv", async (req, res) => {
  const t0 = nowMs();
  log("GET /query-csv params:", req.query);
  const documentId = req.query.documentId;
  const question = req.query.question;
  const mode = req.query.mode;
  const tableName = req.query.tableName || "csvtable";
  const useRowHeaders = String(req.query.useRowHeaders || "false").toLowerCase() === "true";

  if (!documentId || !question) {
    const payload = {
      success: false,
      error: "Missing documentId or question.",
      usage: "GET /api/query-csv?documentId=<id|filename>&question=<text>&mode=nl|sql",
      example: {
        documentId: "Indy 500 Race CSV.json",
        question: "average race time for the past 4 years",
        mode: "nl",
      },
    };
    log("GET /query-csv missing args:", payload);
    return res.status(200).json(payload);
  }

  const result = await handleCsvQuery({ documentId, question, tableName, useRowHeaders, mode });
  const status = result.success ? 200 : 400;
  log("GET /query-csv result:", { status, tookMs: nowMs() - t0, success: result.success, error: result.error });
  return res.status(status).json(result);
});

module.exports = router;
