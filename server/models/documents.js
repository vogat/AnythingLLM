const { v4: uuidv4 } = require("uuid");
const { getVectorDbClass } = require("../utils/helpers");
const prisma = require("../utils/prisma");
const { Telemetry } = require("./telemetry");
const { EventLogs } = require("./eventLogs");
const { safeJsonParse } = require("../utils/http");
const { getModelTag } = require("../endpoints/utils");

console.log("[DEBUG] apiChatHandler.js loaded");

const Document = {
  writable: ["pinned", "watched", "lastUpdatedAt"],
  /**
   * @param {import("@prisma/client").workspace_documents} document - Document PrismaRecord
   * @returns {{
   *  metadata: (null|object),
   *  type: import("./documentSyncQueue.js").validFileType,
   *  source: string
   * }}
   */
  parseDocumentTypeAndSource: function (document) {
    const metadata = safeJsonParse(document.metadata, null);
    if (!metadata) return { metadata: null, type: null, source: null };

    // Parse the correct type of source and its original source path.
    const idx = metadata.chunkSource.indexOf("://");
    const [type, source] = [
      metadata.chunkSource.slice(0, idx),
      metadata.chunkSource.slice(idx + 3),
    ];
    return { metadata, type, source: this._stripSource(source, type) };
  },

  forWorkspace: async function (workspaceId = null) {
    if (!workspaceId) return [];
    return await prisma.workspace_documents.findMany({
      where: { workspaceId },
    });
  },

  delete: async function (clause = {}) {
    try {
      await prisma.workspace_documents.deleteMany({ where: clause });
      return true;
    } catch (error) {
      console.error(error.message);
      return false;
    }
  },

  get: async function (clause = {}) {
    try {
      const document = await prisma.workspace_documents.findFirst({
        where: clause,
      });
      return document || null;
    } catch (error) {
      console.error(error.message);
      return null;
    }
  },

  where: async function (
    clause = {},
    limit = null,
    orderBy = null,
    include = null,
    select = null
  ) {
    try {
      const results = await prisma.workspace_documents.findMany({
        where: clause,
        ...(limit !== null ? { take: limit } : {}),
        ...(orderBy !== null ? { orderBy } : {}),
        ...(include !== null ? { include } : {}),
        ...(select !== null ? { select: { ...select } } : {}),
      });
      return results;
    } catch (error) {
      console.error(error.message);
      return [];
    }
  },

  addDocuments: async function (workspace, additions = [], userId = null) {
    const VectorDb = getVectorDbClass();
    if (additions.length === 0) return { failed: [], embedded: [] };
    const { fileData } = require("../utils/files");
    const embedded = [];
    const failedToEmbed = [];
    const errors = new Set();

    for (const path of additions) {
      const data = await fileData(path);
      if (!data) continue;

      // DEBUG LOG: Print loaded data before CSV chunking check
      console.log('[CSV EMBED] Loaded data:', JSON.stringify(data, null, 2));

      // PATCH: If this is a CSV with row-level chunks, embed each chunk
      if (data.docSource === 'csv-upload' && Array.isArray(data.chunks)) {
        for (const chunk of data.chunks) {
          const docId = uuidv4();
          let { text, metadata } = chunk;
          // DEBUG LOG: Print chunk metadata before embedding
          console.log('[CSV EMBED] Chunk metadata:', JSON.stringify(metadata, null, 2));

          // PATCH: Serialize row_data and ensure all metadata fields are valid
          const safeMetadata = { ...metadata };
          if (safeMetadata.row_data && typeof safeMetadata.row_data !== 'string') {
            safeMetadata.row_data = JSON.stringify(safeMetadata.row_data);
          }
          // Optionally, ensure all fields are strings or primitives
          Object.keys(safeMetadata).forEach(key => {
            if (typeof safeMetadata[key] === 'object' && safeMetadata[key] !== null) {
              safeMetadata[key] = JSON.stringify(safeMetadata[key]);
            }
          });
          if (!text || typeof text !== 'string') text = '';

          // FINAL SANITIZATION: Remove empty/invalid fields and serialize arrays
          Object.keys(safeMetadata).forEach(key => {
            let val = safeMetadata[key];
            // Serialize arrays as JSON strings
            if (Array.isArray(val)) {
              safeMetadata[key] = JSON.stringify(val);
              val = safeMetadata[key];
            }
            if (
              val === undefined ||
              val === null ||
              (typeof val === 'string' && val.trim() === '') ||
              (Array.isArray(val) && val.length === 0)
            ) {
              delete safeMetadata[key];
            }
          });
          if (!text || text.trim() === '') text = '[NO CONTENT]';

          // Remove fields that are empty string, '[]', or '{}'
          Object.keys(safeMetadata).forEach(key => {
            const val = safeMetadata[key];
            if (
              val === '' ||
              val === '[]' ||
              val === '{}' 
            ) {
              delete safeMetadata[key];
            }
          });

          // --- SANITIZE row_data and headers ---
          let row_data = metadata.row_data;
          let headers = metadata.headers;
          // 🚫 Remove numeric keys from row_data
          if (row_data && typeof row_data === 'object') {
            Object.keys(row_data).forEach(k => {
              if (!isNaN(Number(k))) {
                console.warn(`Removing numeric key '${k}' from row_data`);
                delete row_data[k];
              }
            });
          }
          // 🚫 Remove numeric keys from headers
          if (Array.isArray(headers)) {
            for (let i = headers.length - 1; i >= 0; i--) {
              if (!isNaN(Number(headers[i]))) {
                console.warn(`Removing numeric header '${headers[i]}'`);
                headers.splice(i, 1);
              }
            }
          }
          // Helper: Ensure string is at least min UTF-8 bytes
          function ensureMinUtf8Bytes(str, min = 4) {
            const byteLength = Buffer.byteLength(str, 'utf8');
            return byteLength >= min ? str : str + ' '.repeat(min - byteLength);
          }

          // 1. Ensure all fields in row_data are Arrow-safe
          function sanitizeRowData(row) {
            const sanitized = {};
            for (const key in row) {
              let value = row[key];
              if (value === null || value === undefined || String(value).trim().toUpperCase() === "N/A") {
                value = "NULL_";
              } else if (typeof value === "number" || typeof value === "boolean") {
                value = String(value);
              } else if (typeof value !== "string") {
                value = String(value);
              }
              // Guard against problematic symbols
              if (/^[\]}]+$/.test(value.trim())) {
                value = "NULL_";
              }
              // Ensure min 4 bytes
              value = ensureMinUtf8Bytes(value);
              // Fallback for empty/zero-byte
              if (!value || Buffer.byteLength(value, 'utf8') === 0) {
                value = "NULL_";
              }
              // Normalize quotes
              value = value.replace(/\\"/g, '"').replace(/""/g, '"');
              sanitized[key] = value;
            }
            return sanitized;
          }
          if (row_data && typeof row_data === 'object') {
            row_data = sanitizeRowData(row_data);
            safeMetadata.row_data = row_data;
            // 3. Ensure every header exists in row_data
            if (Array.isArray(headers)) {
              headers.forEach(header => {
                if (!(header in row_data)) row_data[header] = "N/A";
              });
            }
            // 4. Debug log: print each field's value and byte length
            Object.entries(row_data).forEach(([k, v]) => {
              const encoded = Buffer.from(v, 'utf8');
              console.log(`[ROW_DATA FIELD] ${k} => '${v}' => ${encoded.length} bytes => [${[...encoded]}]`);
            });
          }

          // MINIMAL PAYLOAD TEST: Only send pageContent, docId, and row_index, but include row_data and headers as native objects if present
          const lancePayload = {
            pageContent: text,
            docId,
            row_index: safeMetadata.row_index
          };
          if (row_data) {
            // Stringify row_data to avoid LanceDB schema errors
            lancePayload.row_data = typeof row_data === 'string' ? row_data : JSON.stringify(row_data);
          }
          if (headers) lancePayload.headers = headers;
          lancePayload.chunkSource = metadata.chunkSource || 'csv-upload';
          console.log('[LANCE PAYLOAD]', JSON.stringify(lancePayload, null, 2));

          // REMOVE headers array field to prevent LanceDB schema errors
          if (lancePayload.headers) {
            delete lancePayload.headers;
            console.log('Removed headers field from lancePayload to avoid schema issues.');
          }

          // 🚫 Remove rogue numeric key '0' from lancePayload
          Object.keys(lancePayload).forEach(key => {
            if (typeof key === "string" && key.trim() === "0") {
              console.warn("Deleting rogue key '0' from payload");
              delete lancePayload[key];
            }
          });
          console.log("Final keys in lancePayload:", Object.keys(lancePayload));

          // 4. Log row_data and payload if embedding fails
          let vectorized, error;
          try {
            ({ vectorized, error } = await VectorDb.addDocumentToNamespace(
              workspace.slug,
              lancePayload,
              path
            ));
          } catch (e) {
            console.error('Failed row_data:', row_data);
            console.error('Failed payload:', lancePayload);
            throw e;
          }

          if (!vectorized) {
            console.error("Failed to vectorize", safeMetadata?.title || newDoc.filename);
            failedToEmbed.push(safeMetadata?.title || newDoc.filename);
            errors.add(error);
            continue;
          }

          try {
            await prisma.workspace_documents.create({
              data: {
                docId: docId,
                docpath: path,
                workspaceId: workspace.id,
                metadata: JSON.stringify(safeMetadata),
                filename: safeMetadata.name || safeMetadata.title || "unknown"
              }
            });
            embedded.push(path + `#row${safeMetadata.row_index}`);
          } catch (error) {
            console.error(error.message);
          }
        }
        continue; // Skip the rest of the loop for CSVs
      }

      // Default: non-CSV document logic
      const docId = uuidv4();
      const { pageContent, ...metadata } = data;
      const newDoc = {
        docId,
        filename: path.split("/")[1],
        docpath: path,
        workspaceId: workspace.id,
        metadata: JSON.stringify(metadata),
      };

      const { vectorized, error } = await VectorDb.addDocumentToNamespace(
        workspace.slug,
        { ...data, docId },
        path
      );

      if (!vectorized) {
        console.error(
          "Failed to vectorize",
          metadata?.title || newDoc.filename
        );
        failedToEmbed.push(metadata?.title || newDoc.filename);
        errors.add(error);
        continue;
      }

      try {
        await prisma.workspace_documents.create({
          data: {
            docId: docId,
            docpath: path,
            workspaceId: workspace.id,
            metadata: typeof metadata === 'string' ? metadata : JSON.stringify(metadata),
            filename: (metadata && metadata.name) ? metadata.name : (metadata && metadata.title ? metadata.title : "unknown")
          }
        });
        embedded.push(path);
      } catch (error) {
        console.error(error.message);
      }
    }

    await Telemetry.sendTelemetry("documents_embedded_in_workspace", {
      LLMSelection: process.env.LLM_PROVIDER || "openai",
      Embedder: process.env.EMBEDDING_ENGINE || "inherit",
      VectorDbSelection: process.env.VECTOR_DB || "lancedb",
      TTSSelection: process.env.TTS_PROVIDER || "native",
      LLMModel: getModelTag(),
    });
    await EventLogs.logEvent(
      "workspace_documents_added",
      {
        workspaceName: workspace?.name || "Unknown Workspace",
        numberOfDocumentsAdded: additions.length,
      },
      userId
    );
    return { failedToEmbed, errors: Array.from(errors), embedded };
  },

  removeDocuments: async function (workspace, removals = [], userId = null) {
    const VectorDb = getVectorDbClass();
    if (removals.length === 0) return;

    for (const path of removals) {
      const document = await this.get({
        docpath: path,
        workspaceId: workspace.id,
      });
      if (!document) continue;
      await VectorDb.deleteDocumentFromNamespace(
        workspace.slug,
        document.docId
      );

      try {
        await prisma.workspace_documents.delete({
          where: { id: document.id, workspaceId: workspace.id },
        });
        await prisma.document_vectors.deleteMany({
          where: { docId: document.docId },
        });
      } catch (error) {
        console.error(error.message);
      }
    }

    await EventLogs.logEvent(
      "workspace_documents_removed",
      {
        workspaceName: workspace?.name || "Unknown Workspace",
        numberOfDocuments: removals.length,
      },
      userId
    );
    return true;
  },

  count: async function (clause = {}, limit = null) {
    try {
      const count = await prisma.workspace_documents.count({
        where: clause,
        ...(limit !== null ? { take: limit } : {}),
      });
      return count;
    } catch (error) {
      console.error("FAILED TO COUNT DOCUMENTS.", error.message);
      return 0;
    }
  },
  update: async function (id = null, data = {}) {
    if (!id) throw new Error("No workspace document id provided for update");

    const validKeys = Object.keys(data).filter((key) =>
      this.writable.includes(key)
    );
    if (validKeys.length === 0)
      return { document: { id }, message: "No valid fields to update!" };

    try {
      const document = await prisma.workspace_documents.update({
        where: { id },
        data,
      });
      return { document, message: null };
    } catch (error) {
      console.error(error.message);
      return { document: null, message: error.message };
    }
  },
  _updateAll: async function (clause = {}, data = {}) {
    try {
      await prisma.workspace_documents.updateMany({
        where: clause,
        data,
      });
      return true;
    } catch (error) {
      console.error(error.message);
      return false;
    }
  },
  content: async function (docId) {
    if (!docId) throw new Error("No workspace docId provided!");
    const document = await this.get({ docId: String(docId) });
    if (!document) throw new Error(`Could not find a document by id ${docId}`);

    const { fileData } = require("../utils/files");
    const data = await fileData(document.docpath);
    return { title: data.title, content: data.pageContent };
  },
  contentByDocPath: async function (docPath) {
    const { fileData } = require("../utils/files");
    const data = await fileData(docPath);
    return { title: data.title, content: data.pageContent };
  },

  // Some data sources have encoded params in them we don't want to log - so strip those details.
  _stripSource: function (sourceString, type) {
    if (["confluence", "github"].includes(type)) {
      const _src = new URL(sourceString);
      _src.search = ""; // remove all search params that are encoded for resync.
      return _src.toString();
    }

    return sourceString;
  },

  /**
   * Functions for the backend API endpoints - not to be used by the frontend or elsewhere.
   * @namespace api
   */
  api: {
    /**
     * Process a document upload from the API and upsert it into the database. This
     * functionality should only be used by the backend /v1/documents/upload endpoints for post-upload embedding.
     * @param {string} wsSlugs - The slugs of the workspaces to embed the document into, will be comma-separated list of workspace slugs
     * @param {string} docLocation - The location/path of the document that was uploaded
     * @returns {Promise<boolean>} - True if the document was uploaded successfully, false otherwise
     */
    uploadToWorkspace: async function (wsSlugs = "", docLocation = null) {
      if (!docLocation)
        return console.log(
          "No document location provided for embedding",
          docLocation
        );

      const slugs = wsSlugs
        .split(",")
        .map((slug) => String(slug)?.trim()?.toLowerCase());
      if (slugs.length === 0)
        return console.log(`No workspaces provided got: ${wsSlugs}`);

      const { Workspace } = require("./workspace");
      const workspaces = await Workspace.where({ slug: { in: slugs } });
      if (workspaces.length === 0)
        return console.log("No valid workspaces found for slugs: ", slugs);

      // Upsert the document into each workspace - do this sequentially
      // because the document may be large and we don't want to overwhelm the embedder, plus on the first
      // upsert we will then have the cache of the document - making n+1 embeds faster. If we parallelize this
      // we will have to do a lot of extra work to ensure that the document is not embedded more than once.
      for (const workspace of workspaces) {
        const { failedToEmbed = [], errors = [] } = await Document.addDocuments(
          workspace,
          [docLocation]
        );
        if (failedToEmbed.length > 0)
          return console.log(
            `Failed to embed document into workspace ${workspace.slug}`,
            errors
          );
        console.log(`Document embedded into workspace ${workspace.slug}...`);
      }

      return true;
    },
  },
};

module.exports = { Document };
