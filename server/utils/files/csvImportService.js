const fs = require("fs");
const path = require("path");
const { v4: uuidv4 } = require("uuid");
const { writeToServerDocuments } = require("../../../collector/utils/files/index");

const loadedCSVDataframes = new Map(); // documentId -> filePath

function importCSVDocument(filePath, workspaceId, useRowHeaders = false) {
  const fileContent = fs.readFileSync(filePath, "utf-8").trim();
  const lines = fileContent.split("\n").filter((l) => l.length > 0);
  if (lines.length < 2) throw new Error("CSV must have at least one header row and one data row");

  const headers = lines[0].split(",").map((h) => h.trim());
  const dataRows = lines.slice(1).map((line) => {
    const values = line.split(",");
    return Object.fromEntries(headers.map((h, i) => [h, values[i]?.trim() || ""]));
  });

  // Remove numeric entries in headers
  const cleanedHeaders = headers.filter(h => !/^\d+$/.test(h));
  // Remove numeric keys from all rows
  let cleanedDataRows = dataRows.map(row => {
    const cleanedRow = {};
    for (const key in row) {
      if (!/^\d+$/.test(key)) {
        cleanedRow[key] = row[key];
      }
    }
    return cleanedRow;
  });

  // Force structuredData.data to be array of objects
  if (!Array.isArray(cleanedDataRows) || typeof cleanedDataRows[0] !== 'object') {
    throw new Error("Invalid CSV structure: data is not an array of objects");
  }
  // Filter out rows with only numeric keys
  cleanedDataRows = cleanedDataRows.filter(row => {
    const keys = Object.keys(row);
    return keys.some(k => isNaN(k)); // keep only rows with non-numeric keys
  });

  const docId = uuidv4();
  const docTitle = path.basename(filePath);

  // For each row, create a chunk with row-specific metadata (no structuredData or full data array)
  const chunks = cleanedDataRows.map((row, index) => {
    // During chunk creation, ensure row_data only contains keys present in headers
    Object.keys(row).forEach(key => {
      if (!cleanedHeaders.includes(key)) delete row[key];
    });
    const rowText = Object.entries(row)
      .map(([key, value]) => `${key}: ${value}`)
      .join(", ");
    return {
      id: uuidv4(),
      text: `Row ${index + 1}: ${rowText}`,
      metadata: {
        id: docId,
        name: docTitle,
        type: "file",
        title: docTitle,
        description: "CSV uploaded document",
        url: `file://${filePath}`,
        docSource: "csv-upload",
        docAuthor: "local",
        workspaceId,
        row_index: index + 1,
        row_data: row,
        headers: cleanedHeaders, // optional, for context
        token_count_estimate: rowText.split(/\s+/).length,
      }
    };
  });

  // Log the first chunk before insertion
  if (chunks[0]) {
    console.log("[DEBUG] First chunk before insertion:", JSON.stringify(chunks[0], null, 2));
    console.log("[DEBUG] First chunk keys:", Object.keys(chunks[0].metadata.row_data || {}));
  }

  // Save the file-level metadata for AnythingLLM document system (for reference)
  const fileLevelMetadata = {
    id: docId,
    name: docTitle,
    type: "file",
    title: docTitle,
    description: "CSV uploaded document",
    url: `file://${filePath}`,
    docSource: "csv-upload",
    docAuthor: "local",
    workspaceId,
    pageContent: chunks.map(c => c.text).join("\n\n"),
    metadata: {
      structuredData: {
        type: "csv",
        headers: cleanedHeaders,
        data: cleanedDataRows
      }
    },
    token_count_estimate: chunks.map(c => c.text).join(" ").split(/\s+/).length,
  };

  const written = writeToServerDocuments(fileLevelMetadata, docTitle.replace(".csv", ""));
  loadedCSVDataframes.set(docId, filePath);

  // Return both the file-level doc and the row-level chunks for embedding
  return { ...written, chunks };
}

module.exports = {
  importCSVDocument,
  loadedCSVDataframes
};
