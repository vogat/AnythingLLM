const { v4: uuidv4 } = require("uuid");
const { reqBody, userFromSession, multiUserMode } = require("../utils/http");
const { validatedRequest } = require("../utils/middleware/validatedRequest");
const { Telemetry } = require("../models/telemetry");
const { streamChatWithWorkspace } = require("../utils/chats/stream");
const {
  ROLES,
  flexUserRoleValid,
} = require("../utils/middleware/multiUserProtected");
const { EventLogs } = require("../models/eventLogs");
const {
  validWorkspaceAndThreadSlug,
  validWorkspaceSlug,
} = require("../utils/middleware/validWorkspace");
const { writeResponseChunk } = require("../utils/helpers/chat/responses");
const { WorkspaceThread } = require("../models/workspaceThread");
const { User } = require("../models/user");
const truncate = require("truncate");
const { getModelTag } = require("./utils");

const { execSync } = require("child_process");
const path = require("path");
const fetch = require("node-fetch");

function runCsvLLMAgent(prompt, filePath) {
  try {
    const scriptPath = path.join(__dirname, "../../../scripts/llm_dataframe_agent.py");
    const command = `python "${scriptPath}" "${filePath}" "${prompt.replace(/"/g, '\\"')}"`;
    const output = execSync(command).toString();
    const result = JSON.parse(output);
    return result.answer || `Error: ${result.error}`;
  } catch (err) {
    return `Python Exec Error: ${err.message}`;
  }
}


function chatEndpoints(app) {
  if (!app) return;

  app.post(
    "/workspace/:slug/stream-chat",
    [validatedRequest, flexUserRoleValid([ROLES.all]), validWorkspaceSlug],
    async (request, response) => {
      console.log("[DEBUG] HANDLER: chat.js /workspace/:slug/stream-chat called");
      console.log("STREAM-CHAT ENDPOINT HIT");
      console.log("[STREAM-CHAT] Request body:", reqBody(request));
      try {
        const user = await userFromSession(request, response);
        const { message, attachments = [] } = reqBody(request);

        if (attachments?.[0]?.filepath?.endsWith(".csv") && attachments[0]?.document?.id) {
          const csvDocId = attachments[0].document.id;
          const csvDocTitle = attachments[0].file?.name || "CSV Document";
          console.log("[CSV HANDLER] Detected CSV attachment:", { csvDocId, csvDocTitle });
          // Call the local /api/query-csv endpoint
          let csvResult, error;
          try {
            console.log("[CSV HANDLER] Calling /api/query-csv with:", { documentId: csvDocId, question: message });
            const res = await fetch("http://localhost:3001/api/query-csv", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ documentId: csvDocId, question: message })
            });
            const data = await res.json();
            console.log("[CSV HANDLER] /api/query-csv response:", data);
            if (!data.success) error = data.error;
            csvResult = data.response || {};
          } catch (e) {
            error = e.message;
            csvResult = {};
            console.log("[CSV HANDLER] Error calling /api/query-csv:", error);
          }

          console.log("[CSV HANDLER] Sending response chunk:", {
            id: uuidv4(),
            type: "final",
            textResponse: error ? `Error: ${error}` : (csvResult.answer ?? csvResult.result ?? csvResult),
            metadata: csvResult.metadata || null,
            sources: [
              {
                id: csvDocId,
                title: csvDocTitle,
                chunkSource: "csv-upload"
              }
            ],
            close: true,
          });

          writeResponseChunk(response, {
            id: uuidv4(),
            type: "final",
            textResponse: error ? `Error: ${error}` : (csvResult.answer ?? csvResult.result ?? csvResult),
            metadata: csvResult.metadata || null,
            sources: [
              {
                id: csvDocId,
                title: csvDocTitle,
                chunkSource: "csv-upload"
              }
            ],
            close: true,
          });

          response.end();
          return;
        }

        const workspace = response.locals.workspace;

        if (!message?.length) {
          response.status(400).json({
            id: uuidv4(),
            type: "abort",
            textResponse: null,
            sources: [],
            close: true,
            error: !message?.length ? "Message is empty." : null,
          });
          return;
        }

        response.setHeader("Cache-Control", "no-cache");
        response.setHeader("Content-Type", "text/event-stream");
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Connection", "keep-alive");
        response.flushHeaders();

        if (multiUserMode(response) && !(await User.canSendChat(user))) {
          writeResponseChunk(response, {
            id: uuidv4(),
            type: "abort",
            textResponse: null,
            sources: [],
            close: true,
            error: `You have met your maximum 24 hour chat quota of ${user.dailyMessageLimit} chats. Try again later.`,
          });
          return;
        }

        await streamChatWithWorkspace(
          response,
          workspace,
          message,
          workspace?.chatMode,
          user,
          null,
          attachments
        );
        await Telemetry.sendTelemetry("sent_chat", {
          multiUserMode: multiUserMode(response),
          LLMSelection: process.env.LLM_PROVIDER || "openai",
          Embedder: process.env.EMBEDDING_ENGINE || "inherit",
          VectorDbSelection: process.env.VECTOR_DB || "lancedb",
          multiModal: Array.isArray(attachments) && attachments?.length !== 0,
          TTSSelection: process.env.TTS_PROVIDER || "native",
          LLMModel: getModelTag(),
        });

        await EventLogs.logEvent(
          "sent_chat",
          {
            workspaceName: workspace?.name,
            chatModel: workspace?.chatModel || "System Default",
          },
          user?.id
        );
        response.end();
      } catch (e) {
        console.error(e);
        writeResponseChunk(response, {
          id: uuidv4(),
          type: "abort",
          textResponse: null,
          sources: [],
          close: true,
          error: e.message,
        });
        response.end();
      }
    }
  );

  app.post(
    "/workspace/:slug/thread/:threadSlug/stream-chat",
    [
      validatedRequest,
      flexUserRoleValid([ROLES.all]),
      validWorkspaceAndThreadSlug,
    ],
    async (request, response) => {
      console.log("[DEBUG] HANDLER: chat.js /workspace/:slug/thread/:threadSlug/stream-chat called");
      try {
        const user = await userFromSession(request, response);
        const { message, attachments = [] } = reqBody(request);

        if (attachments?.[0]?.filepath?.endsWith(".csv") && attachments[0]?.document?.id) {
          const csvDocId = attachments[0].document.id;
          const csvDocTitle = attachments[0].file?.name || "CSV Document";
          console.log("[CSV HANDLER] Detected CSV attachment:", { csvDocId, csvDocTitle });
          // Call the local /api/query-csv endpoint
          let csvResult, error;
          try {
            console.log("[CSV HANDLER] Calling /api/query-csv with:", { documentId: csvDocId, question: message });
            const res = await fetch("http://localhost:3001/api/query-csv", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ documentId: csvDocId, question: message })
            });
            const data = await res.json();
            console.log("[CSV HANDLER] /api/query-csv response:", data);
            if (!data.success) error = data.error;
            csvResult = data.response || {};
          } catch (e) {
            error = e.message;
            csvResult = {};
            console.log("[CSV HANDLER] Error calling /api/query-csv:", error);
          }

          console.log("[CSV HANDLER] Sending response chunk:", {
            id: uuidv4(),
            type: "final",
            textResponse: error ? `Error: ${error}` : (csvResult.answer ?? csvResult.result ?? csvResult),
            metadata: csvResult.metadata || null,
            sources: [
              {
                id: csvDocId,
                title: csvDocTitle,
                chunkSource: "csv-upload"
              }
            ],
            close: true,
          });

          writeResponseChunk(response, {
            id: uuidv4(),
            type: "final",
            textResponse: error ? `Error: ${error}` : (csvResult.answer ?? csvResult.result ?? csvResult),
            metadata: csvResult.metadata || null,
            sources: [
              {
                id: csvDocId,
                title: csvDocTitle,
                chunkSource: "csv-upload"
              }
            ],
            close: true,
          });

          response.end();
          return;
        }

        const workspace = response.locals.workspace;
        const thread = response.locals.thread;

        // --- DYNAMIC TABLE QUERY & DUCKDB LOGIC ---
        const VectorDb = require("../utils/helpers").getVectorDbClass();
        const embeddingsCount = await VectorDb.namespaceCount(workspace.slug);
        console.log("[DEBUG] embeddingsCount:", embeddingsCount);
        let allChunks = [];
        if (embeddingsCount > 0 && embeddingsCount <= 1500) {
          const { Document } = require("../models/documents");
          const allDocs = await Document.forWorkspace(workspace.id);
          allChunks = allDocs.map(doc => {
            let meta = doc.metadata;
            if (typeof meta === 'string') {
              try { meta = JSON.parse(meta); } catch { meta = {}; }
            }
            return {
              text: meta.text || doc.text,
              metadata: meta,
              id: doc.id
            };
          });
          console.log("[DEBUG] allChunks[0]:", allChunks[0]);
          console.log("[CHAT] User query:", message);
          let headers = [];
          if (allChunks.length > 0 && allChunks[0].metadata && allChunks[0].metadata.headers) {
            if (typeof allChunks[0].metadata.headers === "string") {
              try {
                headers = JSON.parse(allChunks[0].metadata.headers);
              } catch {
                headers = [];
              }
            } else {
              headers = allChunks[0].metadata.headers;
            }
          }
          console.log("[DEBUG] headers:", headers);
          const matches = [];
          for (const header of headers) {
            // Try to match "for <header> <value>" or "<header> <value>"
            let regex = new RegExp(`(?:for\\s+)?${header.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&')}[\\s:]*([\\w .'-]+)`, 'i');
            let match = message.match(regex);
            if (match && match[1].trim()) {
              matches.push({ header, value: match[1].trim() });
              continue;
            }
            // Try to match "<header> is <value>"
            regex = new RegExp(`${header.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&')}\\s+is\\s+([\\w .'-]+)`, 'i');
            match = message.match(regex);
            if (match && match[1].trim()) {
              matches.push({ header, value: match[1].trim() });
              continue;
            }
            // Try to match "<header>: <value>"
            regex = new RegExp(`${header.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&')}\\s*:\\s*([\\w .'-]+)`, 'i');
            match = message.match(regex);
            if (match && match[1].trim()) {
              matches.push({ header, value: match[1].trim() });
              continue;
            }
          }
          console.log("[DEBUG] User query:", message);
          console.log("[DEBUG] Matched header/value pairs:", matches);
          if (matches.length === 0) {
            console.log("[DEBUG] No header/value matches found. Skipping DuckDB SQL block and falling through to embedding/vector search.");
          } else {
            console.log("[DEBUG] Entering DuckDB SQL block with matches:", matches);
          }
          // Check if the user message is a SQL query
          let sqlQuery = null;
          if (/^\s*(SELECT|WITH)\b/i.test(message)) {
            sqlQuery = message;
            console.log("[DEBUG] Detected direct SQL query from user:", sqlQuery);
          } else if (matches.length > 0) {
            const tableName = "csvtable";
            if (matches.length > 0) {
                const { header, value } = matches[0];
                const normalizedHeader = header.trim().toLowerCase().replace(/ /g, '_');
                console.log("[DEBUG] Normalized header:", normalizedHeader);
                sqlQuery = `SELECT * FROM ${tableName} WHERE \"${normalizedHeader}\" = '${value}'`;
                console.log("[CHAT] Generated SQL query:", sqlQuery);
            }
          }
          try {
            const { queryCSV } = require("../services/csvQueryService");
            const doc = allDocs[0]; // Use the first doc for now (adjust if needed)
            // Add logging for doc info
            console.log("[DEBUG] DuckDB Query - doc.id:", doc.id);
            console.log("[DEBUG] DuckDB Query - doc.metadata.url:", doc.metadata.url);
            if (doc.metadata.row_data) {
              console.log("[DEBUG] DuckDB Query - sample row_data:", doc.metadata.row_data);
            }
            // Ensure doc.metadata is an object
            let metadata = doc.metadata;
            if (typeof metadata === 'string') {
              try {
                metadata = JSON.parse(metadata);
                console.warn('[DEBUG] Parsed doc.metadata from string:', metadata);
              } catch (e) {
                console.error('[ERROR] Failed to parse doc.metadata string:', doc.metadata);
                metadata = {};
              }
            }
            // Log the full metadata
            console.log('[DEBUG] Full doc.metadata:', metadata);
            // Add warning if name or url is missing
            if (!metadata.name) {
              console.warn('[WARN] doc.metadata.name is missing!');
            }
            if (!metadata.url) {
              console.warn('[WARN] doc.metadata.url is missing!');
            }
            // Use the JSON chunk file as doc_id for queryCSV
            const jsonFileName = metadata.name ? metadata.name.replace(/\.csv$/i, '.json') : (metadata.id + '.json');
            const path = require('path');
            const jsonFilePath = path.resolve(__dirname, '../storage/documents/custom-documents', jsonFileName);
            console.log('[DEBUG] Using JSON chunk file for queryCSV:', jsonFilePath);
            require('../utils/files/csvImportService').loadedCSVDataframes.set(jsonFileName, jsonFilePath);
            // Call queryCSV with the JSON file as doc_id
            const duckDbResult = await queryCSV(jsonFileName, sqlQuery);
            console.log("[CHAT] DuckDB result:", duckDbResult);
            // Add error handling for result
            if (duckDbResult.success && duckDbResult.result && Array.isArray(duckDbResult.result.answer) && duckDbResult.result.answer.length > 0) {
              const row = duckDbResult.result.answer[0];
              const context = `Here is the result for your query:\n${JSON.stringify(row, null, 2)}`;
              require("../utils/helpers/chat/responses").writeResponseChunk(response, {
                uuid: require('uuid').v4(),
                type: "textResponseChunk",
                textResponse: context,
                sources: [{ text: context, metadata: row }],
                close: true,
                error: false
              });
              response.end();
              return;
            } else if (duckDbResult.result && duckDbResult.result.error) {
              // Send error message to user
              require("../utils/helpers/chat/responses").writeResponseChunk(response, {
                uuid: require('uuid').v4(),
                type: "textResponseChunk",
                textResponse: `Error: ${duckDbResult.result.error}`,
                sources: [],
                close: true,
                error: true
              });
              response.end();
              return;
            }
          } catch (err) {
            console.log("[CHAT] DuckDB query error:", err);
          }
        }

        if (!message?.length) {
          response.status(400).json({
            id: uuidv4(),
            type: "abort",
            textResponse: null,
            sources: [],
            close: true,
            error: !message?.length ? "Message is empty." : null,
          });
          return;
        }

        response.setHeader("Cache-Control", "no-cache");
        response.setHeader("Content-Type", "text/event-stream");
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Connection", "keep-alive");
        response.flushHeaders();

        if (multiUserMode(response) && !(await User.canSendChat(user))) {
          writeResponseChunk(response, {
            id: uuidv4(),
            type: "abort",
            textResponse: null,
            sources: [],
            close: true,
            error: `You have met your maximum 24 hour chat quota of ${user.dailyMessageLimit} chats. Try again later.`,
          });
          return;
        }

        await streamChatWithWorkspace(
          response,
          workspace,
          message,
          workspace?.chatMode,
          user,
          thread,
          attachments
        );

        // If thread was renamed emit event to frontend via special `action` response.
        await WorkspaceThread.autoRenameThread({
          thread,
          workspace,
          user,
          newName: truncate(message, 22),
          onRename: (thread) => {
            writeResponseChunk(response, {
              action: "rename_thread",
              thread: {
                slug: thread.slug,
                name: thread.name,
              },
            });
          },
        });

        await Telemetry.sendTelemetry("sent_chat", {
          multiUserMode: multiUserMode(response),
          LLMSelection: process.env.LLM_PROVIDER || "openai",
          Embedder: process.env.EMBEDDING_ENGINE || "inherit",
          VectorDbSelection: process.env.VECTOR_DB || "lancedb",
          multiModal: Array.isArray(attachments) && attachments?.length !== 0,
          TTSSelection: process.env.TTS_PROVIDER || "native",
          LLMModel: getModelTag(),
        });

        await EventLogs.logEvent(
          "sent_chat",
          {
            workspaceName: workspace.name,
            thread: thread.name,
            chatModel: workspace?.chatModel || "System Default",
          },
          user?.id
        );
        response.end();
      } catch (e) {
        console.error(e);
        writeResponseChunk(response, {
          id: uuidv4(),
          type: "abort",
          textResponse: null,
          sources: [],
          close: true,
          error: e.message,
        });
        response.end();
      }
    }
  );
}

module.exports = { chatEndpoints };
