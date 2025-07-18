const { v4: uuidv4 } = require("uuid");
const { DocumentManager } = require("../DocumentManager");
const { WorkspaceChats } = require("../../models/workspaceChats");
const { getVectorDbClass, getLLMProvider } = require("../helpers");
const { writeResponseChunk } = require("../helpers/chat/responses");
const {
  chatPrompt,
  sourceIdentifier,
  recentChatHistory,
  grepAllSlashCommands,
} = require("./index");
const {
  EphemeralAgentHandler,
  EphemeralEventListener,
} = require("../agents/ephemeral");
const { Telemetry } = require("../../models/telemetry");

/**
 * @typedef ResponseObject
 * @property {string} id - uuid of response
 * @property {string} type - Type of response
 * @property {string|null} textResponse - full text response
 * @property {object[]} sources
 * @property {boolean} close
 * @property {string|null} error
 * @property {object} metrics
 */

/**
 * Handle synchronous chats with your workspace via the developer API endpoint
 * @param {{
 *  workspace: import("@prisma/client").workspaces,
 *  message:string,
 *  mode: "chat"|"query",
 *  user: import("@prisma/client").users|null,
 *  thread: import("@prisma/client").workspace_threads|null,
 *  sessionId: string|null,
 *  attachments: { name: string; mime: string; contentString: string }[],
 *  reset: boolean,
 * }} parameters
 * @returns {Promise<ResponseObject>}
 */
async function chatSync({
  workspace,
  message = null,
  mode = "chat",
  user = null,
  thread = null,
  sessionId = null,
  attachments = [],
  reset = false,
}) {
  const uuid = uuidv4();
  const chatMode = mode ?? "chat";

  // If the user wants to reset the chat history we do so pre-flight
  // and continue execution. If no message is provided then the user intended
  // to reset the chat history only and we can exit early with a confirmation.
  if (reset) {
    await WorkspaceChats.markThreadHistoryInvalidV2({
      workspaceId: workspace.id,
      user_id: user?.id,
      thread_id: thread?.id,
      api_session_id: sessionId,
    });
    if (!message?.length) {
      return {
        id: uuid,
        type: "textResponse",
        textResponse: "Chat history was reset!",
        sources: [],
        close: true,
        error: null,
        metrics: {},
      };
    }
  }

  // Process slash commands
  // Since preset commands are not supported in API calls, we can just process the message here
  const processedMessage = await grepAllSlashCommands(message);
  message = processedMessage;

  if (EphemeralAgentHandler.isAgentInvocation({ message })) {
    await Telemetry.sendTelemetry("agent_chat_started");

    // Initialize the EphemeralAgentHandler to handle non-continuous
    // conversations with agents since this is over REST.
    const agentHandler = new EphemeralAgentHandler({
      uuid,
      workspace,
      prompt: message,
      userId: user?.id || null,
      threadId: thread?.id || null,
      sessionId,
    });

    // Establish event listener that emulates websocket calls
    // in Aibitat so that we can keep the same interface in Aibitat
    // but use HTTP.
    const eventListener = new EphemeralEventListener();
    await agentHandler.init();
    await agentHandler.createAIbitat({ handler: eventListener });
    agentHandler.startAgentCluster();

    // The cluster has started and now we wait for close event since
    // this is a synchronous call for an agent, so we return everything at once.
    // After this, we conclude the call as we normally do.
    return await eventListener
      .waitForClose()
      .then(async ({ thoughts, textResponse }) => {
        await WorkspaceChats.new({
          workspaceId: workspace.id,
          prompt: String(message),
          response: {
            text: textResponse,
            sources: [],
            attachments,
            type: chatMode,
            thoughts,
          },
          include: false,
          apiSessionId: sessionId,
        });
        return {
          id: uuid,
          type: "textResponse",
          sources: [],
          close: true,
          error: null,
          textResponse,
          thoughts,
        };
      });
  }

  const LLMConnector = getLLMProvider({
    provider: workspace?.chatProvider,
    model: workspace?.chatModel,
  });
  const VectorDb = getVectorDbClass();
  const messageLimit = workspace?.openAiHistory || 20;
  const hasVectorizedSpace = await VectorDb.hasNamespace(workspace.slug);
  const embeddingsCount = await VectorDb.namespaceCount(workspace.slug);
  // DEBUG: Log embeddingsCount
  console.log("[DEBUG] embeddingsCount:", embeddingsCount);

  // NEW: If the workspace has 500 or fewer embedded chunks, include all as context
  let allChunks = [];
  if (embeddingsCount > 0 && embeddingsCount <= 500) {
    // Fetch all workspace documents
    const { Document } = require("../../models/documents");
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
    // DEBUG: Log first chunk
    console.log("[DEBUG] allChunks[0]:", allChunks[0]);

    // --- LOGGING: User Query ---
    console.log("[CHAT] User query:", message);

    // Try to extract headers from the first chunk
    let headers = [];
    if (allChunks.length > 0 && allChunks[0].metadata && allChunks[0].metadata.headers) {
      headers = allChunks[0].metadata.headers;
    }
    // DEBUG: Log headers
    console.log("[DEBUG] headers:", headers);

    // Try to find header/value pairs in the user query
    const matches = [];
    for (const header of headers) {
      const regex = new RegExp(`${header.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&')}[\s:]*([\w .'-]+)`, 'i');
      const match = message.match(regex);
      if (match) {
        matches.push({ header, value: match[1].trim() });
      }
    }
    // --- LOGGING: Matched Header/Value Pairs ---
    console.log("[CHAT] Matched header/value pairs:", matches);

    // If we found a header/value pair, build a SQL query
    let sqlQuery = null;
    if (matches.length > 0) {
      // For now, just use the first match
      const { header, value } = matches[0];
      sqlQuery = `SELECT * FROM data WHERE "${header}" = '${value}'`;
      // --- LOGGING: Generated SQL Query ---
      console.log("[CHAT] Generated SQL query:", sqlQuery);
      try {
        const { queryCSV } = require("../../services/csvQueryService");
        const doc = allDocs[0]; // Use the first doc for now (adjust if needed)
        const duckDbResult = await queryCSV(doc.id, sqlQuery);
        // --- LOGGING: DuckDB Query Result ---
        console.log("[CHAT] DuckDB result:", duckDbResult);
      } catch (err) {
        console.log("[CHAT] DuckDB query error:", err);
      }
    }
  }

  // User is trying to query-mode chat a workspace that has no data in it - so
  // we should exit early as no information can be found under these conditions.
  if ((!hasVectorizedSpace || embeddingsCount === 0) && chatMode === "query") {
    const textResponse =
      workspace?.queryRefusalResponse ??
      "There is no relevant information in this workspace to answer your query.";

    await WorkspaceChats.new({
      workspaceId: workspace.id,
      prompt: String(message),
      response: {
        text: textResponse,
        sources: [],
        attachments: attachments,
        type: chatMode,
        metrics: {},
      },
      include: false,
      apiSessionId: sessionId,
    });

    return {
      id: uuid,
      type: "textResponse",
      sources: [],
      close: true,
      error: null,
      textResponse,
      metrics: {},
    };
  }

  // If we are here we know that we are in a workspace that is:
  // 1. Chatting in "chat" mode and may or may _not_ have embeddings
  // 2. Chatting in "query" mode and has at least 1 embedding
  let contextTexts = [];
  let sources = [];
  let pinnedDocIdentifiers = [];
  const { rawHistory, chatHistory } = await recentChatHistory({
    user,
    workspace,
    thread,
    messageLimit,
    apiSessionId: sessionId,
  });

  await new DocumentManager({
    workspace,
    maxTokens: LLMConnector.promptWindowLimit(),
  })
    .pinnedDocs()
    .then((pinnedDocs) => {
      pinnedDocs.forEach((doc) => {
        const { pageContent, ...metadata } = doc;
        pinnedDocIdentifiers.push(sourceIdentifier(doc));
        contextTexts.push(doc.pageContent);
        sources.push({
          text:
            pageContent.slice(0, 1_000) +
            "...continued on in source document...",
          ...metadata,
        });
      });
    });

  const vectorSearchResults =
    embeddingsCount !== 0
      ? await VectorDb.performSimilaritySearch({
          namespace: workspace.slug,
          input: message,
          LLMConnector,
          similarityThreshold: workspace?.similarityThreshold,
          topN: workspace?.topN,
          filterIdentifiers: pinnedDocIdentifiers,
          rerank: workspace?.vectorSearchMode === "rerank",
        })
      : {
          contextTexts: [],
          sources: [],
          message: null,
        };

  // If allChunks is populated (<=500), use it for contextTexts and sources
  if (allChunks.length > 0) {
    contextTexts = allChunks.map(chunk => chunk.text);
    sources = allChunks;
  } else {
  const { fillSourceWindow } = require("../helpers/chat");
  const filledSources = fillSourceWindow({
    nDocs: workspace?.topN || 4,
    searchResults: vectorSearchResults.sources,
    history: rawHistory,
    filterIdentifiers: pinnedDocIdentifiers,
  });
  contextTexts = [...contextTexts, ...filledSources.contextTexts];
  sources = [...sources, ...vectorSearchResults.sources];
  }

  // After sources = [...sources, ...vectorSearchResults.sources];
  // Patch: Parse metadata if string and flatten for CSVs
  sources = sources.map(source => {
    let meta = source.metadata;
    if (typeof meta === 'string') {
      try { meta = JSON.parse(meta); } catch { meta = {}; }
    }
    // For CSVs, include row_index and row_data at the top level
    if (meta && meta.docSource === 'csv-upload') {
      return {
        ...source,
        ...meta,
        row_index: meta.row_index,
        row_data: meta.row_data,
        text: source.text || source.pageContent,
        metadata: meta
      };
    }
    return { ...source, metadata: meta };
  });

  // If in query mode and no context chunks are found from search, backfill, or pins -  do not
  // let the LLM try to hallucinate a response or use general knowledge and exit early
  if (chatMode === "query" && contextTexts.length === 0) {
    const textResponse =
      workspace?.queryRefusalResponse ??
      "There is no relevant information in this workspace to answer your query.";

    await WorkspaceChats.new({
      workspaceId: workspace.id,
      prompt: message,
      response: {
        text: textResponse,
        sources: [],
        attachments: attachments,
        type: chatMode,
        metrics: {},
      },
      threadId: thread?.id || null,
      include: false,
      apiSessionId: sessionId,
      user,
    });

    return {
      id: uuid,
      type: "textResponse",
      sources: [],
      close: true,
      error: null,
      textResponse,
      metrics: {},
    };
  }

  // Compress & Assemble message to ensure prompt passes token limit with room for response
  // and build system messages based on inputs and history.
  const messages = await LLMConnector.compressMessages(
    {
      systemPrompt: await chatPrompt(workspace, user),
      userPrompt: message,
      contextTexts,
      chatHistory,
      attachments,
    },
    rawHistory
  );

  // Send the text completion.
  const { textResponse, metrics: performanceMetrics } =
    await LLMConnector.getChatCompletion(messages, {
      temperature: workspace?.openAiTemp ?? LLMConnector.defaultTemp,
    });

  if (!textResponse) {
    return {
      id: uuid,
      type: "abort",
      textResponse: null,
      sources: [],
      close: true,
      error: "No text completion could be completed with this input.",
      metrics: performanceMetrics,
    };
  }

  const { chat } = await WorkspaceChats.new({
    workspaceId: workspace.id,
    prompt: message,
    response: {
      text: textResponse,
      sources,
      attachments,
      type: chatMode,
      metrics: performanceMetrics,
    },
    threadId: thread?.id || null,
    apiSessionId: sessionId,
    user,
  });

  return {
    id: uuid,
    type: "textResponse",
    close: true,
    error: null,
    chatId: chat.id,
    textResponse,
    sources,
    metrics: performanceMetrics,
  };
}

/**
 * Handle streamable HTTP chunks for chats with your workspace via the developer API endpoint
 * @param {{
 * response: import("express").Response,
 *  workspace: import("@prisma/client").workspaces,
 *  message:string,
 *  mode: "chat"|"query",
 *  user: import("@prisma/client").users|null,
 *  thread: import("@prisma/client").workspace_threads|null,
 *  sessionId: string|null,
 *  attachments: { name: string; mime: string; contentString: string }[],
 *  reset: boolean,
 * }} parameters
 * @returns {Promise<VoidFunction>}
 */
async function streamChat({
  response,
  workspace,
  message = null,
  mode = "chat",
  user = null,
  thread = null,
  sessionId = null,
  attachments = [],
  reset = false,
}) {
  // --- DEBUG LOGS ---
  console.log("[DEBUG] streamChat called");
  const uuid = uuidv4();
  const chatMode = mode ?? "chat";

  // If the user wants to reset the chat history we do so pre-flight
  // and continue execution. If no message is provided then the user intended
  // to reset the chat history only and we can exit early with a confirmation.
  if (reset) {
    await WorkspaceChats.markThreadHistoryInvalidV2({
      workspaceId: workspace.id,
      user_id: user?.id,
      thread_id: thread?.id,
      api_session_id: sessionId,
    });
    if (!message?.length) {
      writeResponseChunk(response, {
        id: uuid,
        type: "textResponse",
        textResponse: "Chat history was reset!",
        sources: [],
        attachments: [],
        close: true,
        error: null,
        metrics: {},
      });
      return;
    }
  }

  // Check for and process slash commands
  // Since preset commands are not supported in API calls, we can just process the message here
  const processedMessage = await grepAllSlashCommands(message);
  message = processedMessage;

  if (EphemeralAgentHandler.isAgentInvocation({ message })) {
    await Telemetry.sendTelemetry("agent_chat_started");

    // Initialize the EphemeralAgentHandler to handle non-continuous
    // conversations with agents since this is over REST.
    const agentHandler = new EphemeralAgentHandler({
      uuid,
      workspace,
      prompt: message,
      userId: user?.id || null,
      threadId: thread?.id || null,
      sessionId,
    });

    // Establish event listener that emulates websocket calls
    // in Aibitat so that we can keep the same interface in Aibitat
    // but use HTTP.
    const eventListener = new EphemeralEventListener();
    await agentHandler.init();
    await agentHandler.createAIbitat({ handler: eventListener });
    agentHandler.startAgentCluster();

    // The cluster has started and now we wait for close event since
    // and stream back any results we get from agents as they come in.
    return eventListener
      .streamAgentEvents(response, uuid)
      .then(async ({ thoughts, textResponse }) => {
        await WorkspaceChats.new({
          workspaceId: workspace.id,
          prompt: String(message),
          response: {
            text: textResponse,
            sources: [],
            attachments: attachments,
            type: chatMode,
            thoughts,
          },
          include: true,
          threadId: thread?.id || null,
          apiSessionId: sessionId,
        });
        writeResponseChunk(response, {
          uuid,
          type: "finalizeResponseStream",
          textResponse,
          thoughts,
          close: true,
          error: false,
        });
      });
  }

  const LLMConnector = getLLMProvider({
    provider: workspace?.chatProvider,
    model: workspace?.chatModel,
  });

  const VectorDb = getVectorDbClass();
  const messageLimit = workspace?.openAiHistory || 20;
  const hasVectorizedSpace = await VectorDb.hasNamespace(workspace.slug);
  const embeddingsCount = await VectorDb.namespaceCount(workspace.slug);
  console.log("[DEBUG] embeddingsCount:", embeddingsCount);

  // NEW: If the workspace has 500 or fewer embedded chunks, include all as context
  let allChunks = [];
  if (embeddingsCount > 0 && embeddingsCount <= 500) {
    // Fetch all workspace documents
    const { Document } = require("../../models/documents");
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
      headers = allChunks[0].metadata.headers;
    }
    console.log("[DEBUG] headers:", headers);
    const matches = [];
    for (const header of headers) {
      const regex = new RegExp(`${header.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&')}[\s:]*([\w .'-]+)`, 'i');
      const match = message.match(regex);
      if (match) {
        matches.push({ header, value: match[1].trim() });
      }
    }
    console.log("[CHAT] Matched header/value pairs:", matches);
    let sqlQuery = null;
    if (matches.length > 0) {
      const { header, value } = matches[0];
      sqlQuery = `SELECT * FROM data WHERE "${header}" = '${value}'`;
      console.log("[CHAT] Generated SQL query:", sqlQuery);
      try {
        const { queryCSV } = require("../../services/csvQueryService");
        const doc = allDocs[0]; // Use the first doc for now (adjust if needed)
        const duckDbResult = await queryCSV(doc.id, sqlQuery);
        console.log("[CHAT] DuckDB result:", duckDbResult);
        // If result found, use as context
        if (duckDbResult.success && duckDbResult.result.answer.length > 0) {
          const row = duckDbResult.result.answer[0];
          const context = `Here is the result for your query:\n${JSON.stringify(row, null, 2)}`;
          // Use this as the only context chunk
          // ... existing code to send to LLM ...
          // For streaming, you may need to adjust how contextTexts/sources are set
          // For now, just log and return early for debug
          writeResponseChunk(response, {
            uuid: require('uuid').v4(),
            type: "textResponseChunk",
            textResponse: context,
            sources: [{ text: context, metadata: row }],
            close: true,
            error: false
          });
          return;
        }
      } catch (err) {
        console.log("[CHAT] DuckDB query error:", err);
      }
    }
  }

  // User is trying to query-mode chat a workspace that has no data in it - so
  // we should exit early as no information can be found under these conditions.
  if ((!hasVectorizedSpace || embeddingsCount === 0) && chatMode === "query") {
    const textResponse =
      workspace?.queryRefusalResponse ??
      "There is no relevant information in this workspace to answer your query.";
    writeResponseChunk(response, {
      id: uuid,
      type: "textResponse",
      textResponse,
      sources: [],
      attachments: [],
      close: true,
      error: null,
      metrics: {},
    });
    await WorkspaceChats.new({
      workspaceId: workspace.id,
      prompt: message,
      response: {
        text: textResponse,
        sources: [],
        attachments: attachments,
        type: chatMode,
        metrics: {},
      },
      threadId: thread?.id || null,
      apiSessionId: sessionId,
      include: false,
      user,
    });
    return;
  }

  // If we are here we know that we are in a workspace that is:
  // 1. Chatting in "chat" mode and may or may _not_ have embeddings
  // 2. Chatting in "query" mode and has at least 1 embedding
  let completeText;
  let metrics = {};
  let contextTexts = [];
  let sources = [];
  let pinnedDocIdentifiers = [];
  const { rawHistory, chatHistory } = await recentChatHistory({
    user,
    workspace,
    thread,
    messageLimit,
    apiSessionId: sessionId,
  });

  // Look for pinned documents and see if the user decided to use this feature. We will also do a vector search
  // as pinning is a supplemental tool but it should be used with caution since it can easily blow up a context window.
  // However we limit the maximum of appended context to 80% of its overall size, mostly because if it expands beyond this
  // it will undergo prompt compression anyway to make it work. If there is so much pinned that the context here is bigger than
  // what the model can support - it would get compressed anyway and that really is not the point of pinning. It is really best
  // suited for high-context models.
  await new DocumentManager({
    workspace,
    maxTokens: LLMConnector.promptWindowLimit(),
  })
    .pinnedDocs()
    .then((pinnedDocs) => {
      pinnedDocs.forEach((doc) => {
        const { pageContent, ...metadata } = doc;
        pinnedDocIdentifiers.push(sourceIdentifier(doc));
        contextTexts.push(doc.pageContent);
        sources.push({
          text:
            pageContent.slice(0, 1_000) +
            "...continued on in source document...",
          ...metadata,
        });
      });
    });

  const vectorSearchResults =
    embeddingsCount !== 0
      ? await VectorDb.performSimilaritySearch({
          namespace: workspace.slug,
          input: message,
          LLMConnector,
          similarityThreshold: workspace?.similarityThreshold,
          topN: workspace?.topN,
          filterIdentifiers: pinnedDocIdentifiers,
          rerank: workspace?.vectorSearchMode === "rerank",
        })
      : {
          contextTexts: [],
          sources: [],
          message: null,
        };

  // If allChunks is populated (<=500), use it for contextTexts and sources
  if (allChunks.length > 0) {
    contextTexts = allChunks.map(chunk => chunk.text);
    sources = allChunks;
  } else {
  const { fillSourceWindow } = require("../helpers/chat");
  const filledSources = fillSourceWindow({
    nDocs: workspace?.topN || 4,
    searchResults: vectorSearchResults.sources,
    history: rawHistory,
    filterIdentifiers: pinnedDocIdentifiers,
  });
  contextTexts = [...contextTexts, ...filledSources.contextTexts];
  sources = [...sources, ...vectorSearchResults.sources];
  }

  // After sources = [...sources, ...vectorSearchResults.sources];
  // Patch: Parse metadata if string and flatten for CSVs
  sources = sources.map(source => {
    let meta = source.metadata;
    if (typeof meta === 'string') {
      try { meta = JSON.parse(meta); } catch { meta = {}; }
    }
    // For CSVs, include row_index and row_data at the top level
    if (meta && meta.docSource === 'csv-upload') {
      return {
        ...source,
        ...meta,
        row_index: meta.row_index,
        row_data: meta.row_data,
        text: source.text || source.pageContent,
        metadata: meta
      };
    }
    return { ...source, metadata: meta };
  });

  // If in query mode and no context chunks are found from search, backfill, or pins -  do not
  // let the LLM try to hallucinate a response or use general knowledge and exit early
  if (chatMode === "query" && contextTexts.length === 0) {
    const textResponse =
      workspace?.queryRefusalResponse ??
      "There is no relevant information in this workspace to answer your query.";
    writeResponseChunk(response, {
      id: uuid,
      type: "textResponse",
      textResponse,
      sources: [],
      close: true,
      error: null,
      metrics: {},
    });

    await WorkspaceChats.new({
      workspaceId: workspace.id,
      prompt: message,
      response: {
        text: textResponse,
        sources: [],
        attachments: attachments,
        type: chatMode,
        metrics: {},
      },
      threadId: thread?.id || null,
      apiSessionId: sessionId,
      include: false,
      user,
    });
    return;
  }

  // Compress & Assemble message to ensure prompt passes token limit with room for response
  // and build system messages based on inputs and history.
  const messages = await LLMConnector.compressMessages(
    {
      systemPrompt: await chatPrompt(workspace, user),
      userPrompt: message,
      contextTexts,
      chatHistory,
      attachments,
    },
    rawHistory
  );

  // If streaming is not explicitly enabled for connector
  // we do regular waiting of a response and send a single chunk.
  if (LLMConnector.streamingEnabled() !== true) {
    console.log(
      `\x1b[31m[STREAMING DISABLED]\x1b[0m Streaming is not available for ${LLMConnector.constructor.name}. Will use regular chat method.`
    );
    const { textResponse, metrics: performanceMetrics } =
      await LLMConnector.getChatCompletion(messages, {
        temperature: workspace?.openAiTemp ?? LLMConnector.defaultTemp,
      });
    completeText = textResponse;
    metrics = performanceMetrics;
    writeResponseChunk(response, {
      uuid,
      sources,
      type: "textResponseChunk",
      textResponse: completeText,
      close: true,
      error: false,
      metrics,
    });
  } else {
    const stream = await LLMConnector.streamGetChatCompletion(messages, {
      temperature: workspace?.openAiTemp ?? LLMConnector.defaultTemp,
    });
    completeText = await LLMConnector.handleStream(response, stream, {
      uuid,
      sources,
    });
    metrics = stream.metrics;
  }

  if (completeText?.length > 0) {
    const { chat } = await WorkspaceChats.new({
      workspaceId: workspace.id,
      prompt: message,
      response: {
        text: completeText,
        sources,
        type: chatMode,
        metrics,
        attachments,
      },
      threadId: thread?.id || null,
      apiSessionId: sessionId,
      user,
    });

    writeResponseChunk(response, {
      uuid,
      type: "finalizeResponseStream",
      close: true,
      error: false,
      chatId: chat.id,
      metrics,
    });
    return;
  }

  writeResponseChunk(response, {
    uuid,
    type: "finalizeResponseStream",
    close: true,
    error: false,
  });
  return;
}

module.exports.ApiChatHandler = {
  chatSync,
  streamChat,
};
