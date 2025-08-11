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
            textResponse: (() => {
              if (error) return `Error: ${error}`;
              const base = csvResult.answer ?? csvResult.result ?? csvResult;
              const code = csvResult.code || (csvResult.response && csvResult.response.code);
              if (code) return `${typeof base === 'string' ? base : JSON.stringify(base)}\n\nCode used (from PandasAI):\n\n\`\`\`python\n${code}\n\`\`\``;
              return base;
            })(),
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
            textResponse: (() => {
              if (error) return `Error: ${error}`;
              const base = csvResult.answer ?? csvResult.result ?? csvResult;
              const code = csvResult.code || (csvResult.response && csvResult.response.code);
              if (code) return `${typeof base === 'string' ? base : JSON.stringify(base)}\n\nCode used (from PandasAI):\n\n\`\`\`python\n${code}\n\`\`\``;
              return base;
            })(),
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
            textResponse: (() => {
              if (error) return `Error: ${error}`;
              const base = csvResult.answer ?? csvResult.result ?? csvResult;
              const code = csvResult.code || (csvResult.response && csvResult.response.code);
              if (code) return `${typeof base === 'string' ? base : JSON.stringify(base)}\n\nCode used (from PandasAI):\n\n\`\`\`python\n${code}\n\`\`\``;
              return base;
            })(),
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
            textResponse: (() => {
              if (error) return `Error: ${error}`;
              const base = csvResult.answer ?? csvResult.result ?? csvResult;
              const code = csvResult.code || (csvResult.response && csvResult.response.code);
              if (code) return `${typeof base === 'string' ? base : JSON.stringify(base)}\n\nCode used (from PandasAI):\n\n\`\`\`python\n${code}\n\`\`\``;
              return base;
            })(),
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
            const escaped = header.replace(/[-\/\\^$*+?.()|[\]{}]/g, "\\$&");
            // Try to match "for <header> <value>" or "<header> <value>" with word boundaries
            let regex = new RegExp(`(?:for\\s+)?\\b${escaped}\\b[\\s:]*([\\w .'-]+)`, 'i');
            let match = message.match(regex);
            if (match && match[1].trim()) {
              matches.push({ header, value: match[1].trim() });
              continue;
            }
            // Try to match "<header> is <value>"
            regex = new RegExp(`\\b${escaped}\\b\\s+is\\s+([\\w .'-]+)`, 'i');
            match = message.match(regex);
            if (match && match[1].trim()) {
              matches.push({ header, value: match[1].trim() });
              continue;
            }
            // Try to match "<header>: <value>"
            regex = new RegExp(`\\b${escaped}\\b\\s*:\\s*([\\w .'-]+)`, 'i');
            match = message.match(regex);
            if (match && match[1].trim()) {
              matches.push({ header, value: match[1].trim() });
              continue;
            }
          }
          console.log("[DEBUG] User query:", message);
          console.log("[DEBUG] Matched header/value pairs:", matches);

          // Detect aggregation/range intent to route to PandasAI
          const hasAggregation = /\b(average|avg|mean|sum|count|min|max|median|std|variance|percent|percentage|total)\b/i.test(message);
          const pastYearsMatch = message.match(/past\s+(\d+)\s+years?/i);
          const explicitYearMatch = message.match(/\b(19\d{2}|20\d{2})\b/g);

          if (matches.length === 0) {
            console.log("[DEBUG] No header/value matches found. Skipping DuckDB SQL block and falling through to embedding/vector search.");
          } else {
            console.log("[DEBUG] Entering DuckDB SQL block with matches:", matches);
          }
          // Check if the user message is a SQL query
          let sqlQuery = null;
          const isDirectSql = /^\s*(SELECT|WITH)\b/i.test(message);
          if (isDirectSql) {
            sqlQuery = message;
            console.log("[DEBUG] Detected direct SQL query from user:", sqlQuery);
          } else if (!hasAggregation) {
            const tableName = "csvtable";
            // Prefer explicit year queries like "in 1998" or "1998"
            if (explicitYearMatch && explicitYearMatch.length > 0) {
              const year = parseInt(explicitYearMatch[explicitYearMatch.length - 1], 10);
              let selectCols = '*';
              if (/race\s*time/i.test(message)) selectCols = '"race_time"';
              else if (/race\s*speed/i.test(message)) selectCols = '"race_speed"';
              else if (/driver|winner/i.test(message)) selectCols = '"driver"';
              console.log('[DEBUG] Explicit year detected:', year, 'selectCols:', selectCols);
              sqlQuery = `SELECT ${selectCols} FROM ${tableName} WHERE "year" = ${year}`;
              console.log("[CHAT] Generated SQL query (explicit year):", sqlQuery);
            } else if (!pastYearsMatch && matches.length > 0) {
              const { header, value } = matches[0];
              const normalizedHeader = header.trim().toLowerCase().replace(/ /g, '_');
              console.log("[DEBUG] Normalized header:", normalizedHeader);
              sqlQuery = `SELECT * FROM ${tableName} WHERE \"${normalizedHeader}\" = '${value}'`;
              console.log("[CHAT] Generated SQL query (header/value):", sqlQuery);
            }
          }

          try {
            const { queryCSV } = require("../services/csvQueryService");
            const doc = allDocs[0]; // Use the first doc for now (adjust if needed)
            console.log("[DEBUG] DuckDB/PandasAI - doc.id:", doc.id);
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
            console.log('[DEBUG] Full doc.metadata:', metadata);

            // Prefer PandasAI for aggregation/range or when SQL is not generated
            if (!isDirectSql && (hasAggregation || pastYearsMatch) && !sqlQuery) {
              console.log('[DEBUG] Routing to PandasAI via /api/query-csv for NL question');
              // Prefer passing the JSON filename to avoid DB lookups and resolve ambiguity
              let fileKey = null;
              if (metadata && (metadata.name || metadata.title)) {
                const baseName = (metadata.name || metadata.title);
                fileKey = baseName.replace(/\.csv$/i, '.json');
              }
              const payload = {
                documentId: fileKey || doc.id,
                question: message,
                mode: 'nl'
              };
              console.log('[DEBUG] /api/query-csv payload:', payload);

              // Add timeout so chat does not hang if PandasAI/Ollama stalls
              const { AbortController } = require('abort-controller');
              const controller = new AbortController();
              const startTs = Date.now();
              const timeout = setTimeout(() => {
                console.log('[DEBUG] /api/query-csv aborting after ms:', Date.now() - startTs);
                controller.abort();
              }, 12000);
              let data;
              try {
                const res = await fetch("http://localhost:3001/api/query-csv", {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify(payload),
                  signal: controller.signal
                });
                clearTimeout(timeout);
                data = await res.json();
                console.log('[DEBUG] /api/query-csv roundtrip ms:', Date.now() - startTs);
              } catch (e) {
                clearTimeout(timeout);
                console.log('[DEBUG] /api/query-csv request failed after ms:', Date.now() - startTs, 'error:', e.message);
                data = { success: false, error: e.message };
              }
              console.log('[DEBUG] /api/query-csv (PandasAI) response:', data);
              if (data.success && data.response) {
                const base = data.response.answer ?? data.response.result ?? data.response;
                const code = data.response.code;
                const text = `${typeof base === 'string' ? base : JSON.stringify(base)}${code ? `\n\nCode used (from PandasAI):\n\n\`\`\`python\n${code}\n\`\`\`` : ''}`;
                require("../utils/helpers/chat/responses").writeResponseChunk(response, {
                  uuid: require('uuid').v4(),
                  type: "textResponseChunk",
                  textResponse: text,
                  sources: [{ text, metadata: { docId: doc.id, name: metadata.name } }],
                  close: true,
                  error: false
                });
                response.end();
                return;
              }
              if (!data.success) {
                // Surface the error so the user sees why it failed
                const errText = `CSV NL query failed: ${data.error || 'Unknown error'}`;
                require("../utils/helpers/chat/responses").writeResponseChunk(response, {
                  uuid: require('uuid').v4(),
                  type: "textResponseChunk",
                  textResponse: errText,
                  sources: [],
                  close: true,
                  error: true
                });
                response.end();
                return;
              }
            }

            if (sqlQuery) {
              // Use the JSON chunk file as doc_id for queryCSV
              const jsonFileName = metadata.name ? metadata.name.replace(/\.csv$/i, '.json') : (metadata.id + '.json');
              const path = require('path');
              const jsonFilePath = path.resolve(__dirname, '../storage/documents/custom-documents', jsonFileName);
              console.log('[DEBUG] Using JSON chunk file for queryCSV:', jsonFilePath);
              require('../utils/files/csvImportService').loadedCSVDataframes.set(jsonFileName, jsonFilePath);
              // Call queryCSV with the JSON file as doc_id
              const duckDbResult = await queryCSV(jsonFileName, sqlQuery);
              console.log("[CHAT] DuckDB result:", duckDbResult);
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
            }
          } catch (err) {
            console.log("[CHAT] Data query error:", err);
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
