# PicoClaw Architecture Flow

This document provides a detailed walkthrough of how PicoClaw processes messages, from user input to response delivery.

---

## Table of Contents

1. [Overview](#overview)
2. [Message Bus Architecture](#message-bus-architecture)
3. [Inbound Message Flow](#inbound-message-flow)
4. [Outbound Message Flow](#outbound-message-flow)
5. [Agent Command Flow](#agent-command-flow)
6. [Turn Execution Loop](#turn-execution-loop)
7. [Context Building](#context-building)
8. [Tool Execution](#tool-execution)
9. [Session Management](#session-management)

---

## Overview

PicoClaw follows a modular, event-driven architecture with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────────┐
│                         Gateway (Entry Point)                    │
│  - Loads configuration                                           │
│  - Initializes all services                                      │
│  - Manages lifecycle                                             │
└───────────────┬────────────────────────────────┬─────────────────┘
                │                                │
                ▼                                ▼
┌───────────────────────────┐    ┌───────────────────────────────┐
│     Channel Adapters      │    │        Agent Loop             │
│  Telegram, Discord, etc.  │◀──▶│  LLM calls, Tool execution    │
└───────────────────────────┘    └───────────────────────────────┘
                │                                │
                └────────────┬───────────────────┘
                             ▼
                   ┌─────────────────┐
                   │   Message Bus   │
                   │  (Pub/Sub)      │
                   └─────────────────┘
```

### Request Flow Summary

1. **Inbound**: User sends message → Channel adapter receives → Publishes to Message Bus
2. **Processing**: Agent Loop consumes → Loads session history → Calls LLM with tools
3. **Tool Execution**: LLM requests tools → Agent executes → Results returned to LLM
4. **Outbound**: LLM generates response → Published to Message Bus → Channel adapter sends to user

---

## Message Bus Architecture

The Message Bus (`pkg/bus/`) decouples channels from agents using an asynchronous pub-sub pattern.

### Structure

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                              MESSAGE BUS (pkg/bus/)                               │
│                                                                                   │
│  ┌─────────────────────┐                        ┌──────────────────────┐         │
│  │  inbound channel    │ ◀────────────────────  │  PublishInbound()    │         │
│  │  (chan InboundMsg)  │                        └──────────────────────┘         │
│  │  buffer: 64         │                                                          │
│  └─────────┬───────────┘                                                          │
│            │                                                                      │
│            ▼                                                                      │
│  ┌─────────────────────┐                        ┌──────────────────────┐         │
│  │  InboundChan()      │ ────────────────────▶  │  AgentLoop.Run()     │         │
│  │  (read-only)        │                        └──────────────────────┘         │
│  └─────────────────────┘                                                          │
│                                                                                   │
│  ┌─────────────────────┐                        ┌──────────────────────┐         │
│  │  outbound channel   │ ◀────────────────────  │  PublishOutbound()   │         │
│  │  (chan OutboundMsg) │                        └──────────────────────┘         │
│  │  buffer: 64         │                                                          │
│  └─────────┬───────────┘                                                          │
│            │                                                                      │
│            ▼                                                                      │
│  ┌─────────────────────┐                        ┌──────────────────────┐         │
│  │  OutboundChan()     │ ────────────────────▶  │  Manager.dispatch()  │         │
│  │  (read-only)        │                        └──────────────────────┘         │
│  └─────────────────────┘                                                          │
│                                                                                   │
│  ┌─────────────────────┐  (Same pattern for media)                               │
│  │  outboundMedia chan │                                                          │
│  └─────────────────────┘                                                          │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Message Types

#### InboundMessage (User → Agent)

```go
type InboundMessage struct {
    Channel    string            // "telegram", "discord", "slack"
    SenderID   string            // canonical sender ID
    Sender     SenderInfo        // structured sender info
    ChatID     string            // chat/conversation ID
    Content    string            // text content
    Media      []string          // media:// refs for images/audio/files
    Peer       Peer              // routing peer (direct/group/channel)
    MessageID  string            // platform message ID (for replies)
    MediaScope string            // media lifecycle scope
    SessionKey string            // explicit session override
    Metadata   map[string]string // platform-specific metadata
}
```

#### OutboundMessage (Agent → User)

```go
type OutboundMessage struct {
    Channel          string  // target channel
    ChatID           string  // target chat
    Content          string  // text to send
    ReplyToMessageID string  // optional reply-to
}
```

#### OutboundMediaMessage (Agent → User with attachments)

```go
type OutboundMediaMessage struct {
    Channel string       // target channel
    ChatID  string       // target chat
    Parts   []MediaPart  // attachments (type, ref, caption, filename)
}
```

---

## Inbound Message Flow

```
┌─────────────────┐
│ Platform Event  │  (e.g. Telegram update, Discord message)
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────┐
│ Channel.handleMessage()             │
│  1. Parse platform event            │
│  2. Check allowlist                 │
│  3. Download attachments → media:// │
│  4. Apply group trigger filtering   │
│  5. Build bus.InboundMessage        │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│ BaseChannel.HandleMessage()         │
│  1. Final allowlist check           │
│  2. Start typing indicator          │
│  3. Add thinking reaction (👀)      │
│  4. Send placeholder ("Thinking…")  │
│  5. Record all for later cleanup    │
│  6. bus.PublishInbound(msg)         │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│ MessageBus.inbound channel          │
│  (buffered, capacity 64)            │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│ AgentLoop.Run()                     │
│  ← msg := <-bus.InboundChan()       │
│  1. Start steering drain goroutine  │
│  2. Process message                 │
│  3. Handle steering continuation    │
│  4. Publish response                │
└─────────────────────────────────────┘
```

### Example: Telegram Inbound Flow

```go
// telegram.go: handleMessage processes incoming Telegram updates
func (c *TelegramChannel) handleMessage(ctx context.Context, message *telego.Message) error {
    // 1. Extract sender info
    sender := bus.SenderInfo{
        Platform:    "telegram",
        PlatformID:  fmt.Sprintf("%d", user.ID),
        CanonicalID: "telegram:123456",
        Username:    user.Username,
    }

    // 2. Check allowlist early
    if !c.IsAllowedSender(sender) {
        return nil  // silently ignore
    }

    // 3. Download media → store in MediaStore → get refs
    photoPath := c.downloadPhoto(ctx, photo.FileID)
    ref := store.Store(photoPath, meta, scope)  // → "media://abc123"
    mediaPaths = append(mediaPaths, ref)

    // 4. Group trigger filtering
    if message.Chat.Type != "private" {
        respond, cleaned := c.ShouldRespondInGroup(isMentioned, content)
        if !respond { return nil }
    }

    // 5. Call BaseChannel.HandleMessage → PublishInbound
    c.HandleMessage(ctx, peer, messageID, senderID, chatID, content, mediaPaths, metadata, sender)
}
```

---

## Outbound Message Flow

```
┌─────────────────────────────────────┐
│ AgentLoop.runTurn()                 │
│  response = LLM.Chat(...)           │
│  bus.PublishOutbound(response)      │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│ MessageBus.outbound channel         │
│  (buffered, capacity 64)            │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│ Manager.dispatchOutbound()          │
│  for msg := range bus.OutboundChan()│
│  1. Skip internal channels          │
│  2. Find channel worker             │
│  3. Enqueue to worker.queue         │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│ Manager.runWorker()                 │
│  for msg := range worker.queue:     │
│  1. Split by <|[SPLIT]|> marker     │
│  2. Split by MaxMessageLength       │
│  3. For each chunk: sendWithRetry() │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│ Manager.sendWithRetry()             │
│  1. Rate limit (wait for token)     │
│  2. preSend():                      │
│     ├─ Stop typing indicator        │
│     ├─ Remove reaction              │
│     └─ Edit placeholder (if exists) │
│  3. If placeholder edited → done    │
│  4. Else: channel.Send(msg)         │
│  5. Retry with backoff on failure   │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│ TelegramChannel.Send()              │
│  1. Parse chatID/threadID           │
│  2. Convert markdown → HTML         │
│  3. bot.SendMessage()               │
└─────────────────────────────────────┘
```

### Worker Architecture (Per-Channel)

```
┌────────────────────────────────────────────────────────────────────┐
│                        Channel Manager                              │
├────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                │
│  │  telegram   │  │   discord   │  │    slack    │   ...          │
│  │   worker    │  │   worker    │  │   worker    │                │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                │
│         │                │                │                        │
│  ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐                │
│  │rate limiter │  │rate limiter │  │rate limiter │                │
│  │ 20 msg/sec  │  │  1 msg/sec  │  │  1 msg/sec  │                │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                │
│         │                │                │                        │
│  ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐                │
│  │   queue     │  │   queue     │  │   queue     │                │
│  │ (buffer 16) │  │ (buffer 16) │  │ (buffer 16) │                │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                │
│         │                │                │                        │
│  ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐                │
│  │sendWithRetry│  │sendWithRetry│  │sendWithRetry│                │
│  │(exp backoff)│  │(exp backoff)│  │(exp backoff)│                │
│  └─────────────┘  └─────────────┘  └─────────────┘                │
└────────────────────────────────────────────────────────────────────┘
```

### Retry Strategy

```go
// Error classification:
// - ErrNotRunning / ErrSendFailed → permanent, no retry
// - ErrRateLimit → fixed 1s delay retry
// - ErrTemporary / unknown → exponential backoff (500ms → 8s max)

for attempt := 0; attempt <= maxRetries; attempt++ {
    err = channel.Send(ctx, msg)
    if err == nil { return }

    if errors.Is(err, ErrNotRunning) || errors.Is(err, ErrSendFailed) {
        break  // permanent failure
    }

    if errors.Is(err, ErrRateLimit) {
        time.Sleep(rateLimitDelay)  // fixed 1s
        continue
    }

    // Exponential backoff for transient errors
    backoff := min(baseBackoff * 2^attempt, maxBackoff)
    time.Sleep(backoff)
}
```

---

## Agent Command Flow

The `picoclaw agent` command provides direct CLI access to the agent.

### Full Flow Diagram

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                              CLI ENTRY POINT                                    │
│                                                                                 │
│  $ picoclaw agent -m "What time is it?"  (or interactive mode)                 │
└───────────────────────────────────┬────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│  main.go                                                                        │
│  ├─ Print banner                                                               │
│  └─ NewPicoclawCommand().Execute()                                             │
│        └─ agent.NewAgentCommand() → agentCmd()                                 │
└───────────────────────────────────┬────────────────────────────────────────────┘
                                    │
                                    ▼
```

### Phase 1: Configuration Loading

```
┌────────────────────────────────────────────────────────────────────────────────┐
│  agentCmd() [cmd/picoclaw/internal/agent/helpers.go]                           │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  1. internal.LoadConfig()                                                       │
│     ├─ Find config: $PICOCLAW_HOME/config.json or ~/.picoclaw/config.json     │
│     ├─ Parse JSON with env var substitution (${VAR})                           │
│     └─ Set log level from config                                               │
│                                                                                 │
│  2. Configure logging (debug mode if -d flag)                                  │
│                                                                                 │
│  3. Override model if --model flag provided                                    │
│                                                                                 │
└───────────────────────────────────┬────────────────────────────────────────────┘
                                    │
                                    ▼
```

### Phase 2: Provider Creation

```
┌────────────────────────────────────────────────────────────────────────────────┐
│  providers.CreateProvider(cfg) [pkg/providers/legacy_provider.go]              │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  1. Get model name from cfg.Agents.Defaults.ModelName                          │
│                                                                                 │
│  2. Look up model in cfg.ModelList (e.g., "gpt-4o", "claude-sonnet")           │
│                                                                                 │
│  3. CreateProviderFromConfig(modelConfig)                                      │
│     ├─ "openai" → OpenAI/OpenAI-compat provider                               │
│     ├─ "anthropic" → Anthropic Claude provider                                 │
│     ├─ "bedrock" → AWS Bedrock provider                                        │
│     ├─ "azure" → Azure OpenAI provider                                         │
│     └─ etc.                                                                     │
│                                                                                 │
│  Returns: provider (LLMProvider interface), modelID                            │
└───────────────────────────────────┬────────────────────────────────────────────┘
                                    │
                                    ▼
```

### Phase 3: Agent Loop Initialization

```
┌────────────────────────────────────────────────────────────────────────────────┐
│  agent.NewAgentLoop(cfg, msgBus, provider) [pkg/agent/loop.go]                 │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  1. NewAgentRegistry(cfg, provider)                                            │
│     ├─ For each agent in cfg.Agents.List (or implicit "main"):                │
│     │    └─ NewAgentInstance(agentCfg, defaults, cfg, provider)               │
│     │         ├─ Resolve workspace path                                        │
│     │         ├─ Create ToolRegistry with enabled tools:                       │
│     │         │    read_file, write_file, list_dir, exec, edit_file, ...      │
│     │         ├─ Create SessionStore (JSONL sessions in workspace/sessions/)  │
│     │         ├─ Create ContextBuilder (loads AGENT.md, SOUL.md, skills)       │
│     │         └─ Set up model routing (light/heavy) if configured             │
│     └─ Create RouteResolver for multi-agent routing                           │
│                                                                                 │
│  2. NewFallbackChain(cooldown) — for automatic model failover                  │
│                                                                                 │
│  3. NewEventBus() — for hooks & event subscribers                              │
│                                                                                 │
│  4. NewHookManager(eventBus) — registers configured hooks                      │
│                                                                                 │
│  5. registerSharedTools() — web_search, send_message, spawn, cron, etc.       │
│                                                                                 │
│  Returns: *AgentLoop (ready but not running)                                   │
└───────────────────────────────────┬────────────────────────────────────────────┘
                                    │
                                    ▼
```

### Phase 4: Message Processing

```
┌────────────────────────────────────────────────────────────────────────────────┐
│  Single Message Mode (picoclaw agent -m "message")                              │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  agentLoop.ProcessDirect(ctx, message, sessionKey)                             │
│     │                                                                           │
│     └─ ProcessDirectWithChannel(ctx, content, sessionKey, "cli", "direct")     │
│           │                                                                     │
│           ├─ ensureHooksInitialized() — load configured hooks                  │
│           ├─ ensureMCPInitialized() — connect to MCP servers                   │
│           │                                                                     │
│           └─ processMessage(ctx, InboundMessage{                               │
│                  Channel: "cli",                                                │
│                  SenderID: "cron",                                              │
│                  ChatID: "direct",                                              │
│                  Content: message,                                              │
│                  SessionKey: sessionKey                                         │
│              })                                                                 │
└───────────────────────────────────┬────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│  processMessage() [pkg/agent/loop.go]                                          │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  1. Log message preview                                                         │
│                                                                                 │
│  2. Transcribe audio if present (via Whisper)                                  │
│                                                                                 │
│  3. resolveMessageRoute(msg) → which agent handles this?                       │
│     └─ Returns: route (channel + peer → sessionKey), *AgentInstance            │
│                                                                                 │
│  4. Check for /commands (like /use python, /help, /list skills)               │
│                                                                                 │
│  5. Build processOptions{                                                       │
│        SessionKey, Channel, ChatID, SenderID,                                  │
│        UserMessage, Media, EnableSummary, ...                                  │
│     }                                                                           │
│                                                                                 │
│  6. runAgentLoop(ctx, agent, opts)                                             │
│                                                                                 │
└───────────────────────────────────┬────────────────────────────────────────────┘
                                    │
                                    ▼
```

---

## Turn Execution Loop

The core of message processing happens in `runTurn()`.

### Setup Phase

```
┌────────────────────────────────────────────────────────────────────────────────┐
│  runTurn() [pkg/agent/loop.go]                                                 │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  SETUP:                                                                         │
│  ├─ Create turnState with cancellation                                         │
│  ├─ Register active turn for concurrency tracking                              │
│  ├─ Emit EventKindTurnStart                                                    │
│  ├─ Load session history: agent.Sessions.GetHistory(sessionKey)               │
│  └─ Build context messages:                                                    │
│       agent.ContextBuilder.BuildMessages(history, summary, userMessage, ...)   │
│       ├─ Cached system prompt (identity, AGENT.md, SOUL.md, skills, memory)   │
│       ├─ Dynamic context (time, runtime, session info)                         │
│       ├─ Summary of prior conversation (if any)                                │
│       └─ User message                                                          │
│                                                                                 │
│  PROACTIVE COMPRESSION:                                                         │
│  └─ If context > budget → forceCompression() before LLM call                   │
│                                                                                 │
│  SAVE USER MESSAGE to session history                                          │
│                                                                                 │
│  SELECT MODEL CANDIDATES via selectCandidates()                                │
│  └─ May route to light model if message is simple                              │
│                                                                                 │
└───────────────────────────────────┬────────────────────────────────────────────┘
                                    │
                                    ▼
```

### Iteration Loop

```
┌────────────────────────────────────────────────────────────────────────────────┐
│  ITERATION LOOP (turnLoop)                                                      │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  for iteration < maxIterations:                                                │
│                                                                                 │
│    ┌──────────────────────────────────────────────────────────────────────┐   │
│    │ 1. CHECK INTERRUPTS                                                   │   │
│    │    ├─ Hard abort requested? → abortTurn()                            │   │
│    │    └─ Poll steering queue for new messages                           │   │
│    └──────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│    ┌──────────────────────────────────────────────────────────────────────┐   │
│    │ 2. INJECT PENDING MESSAGES (steering, SubTurn results)               │   │
│    │    └─ Append to messages[], save to session, emit event              │   │
│    └──────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│    ┌──────────────────────────────────────────────────────────────────────┐   │
│    │ 3. HOOKS: BeforeLLM                                                   │   │
│    │    └─ Can modify messages/model/tools or abort turn                  │   │
│    └──────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│    ┌──────────────────────────────────────────────────────────────────────┐   │
│    │ 4. LLM CALL (with retry & fallback)                                   │   │
│    │                                                                       │   │
│    │    callLLM(messages, tools) → provider.Chat(...)                      │   │
│    │                                                                       │   │
│    │    RETRY LOGIC (up to 2 retries):                                     │   │
│    │    ├─ Timeout error → exponential backoff, retry                     │   │
│    │    ├─ Context exceeded → forceCompression(), rebuild, retry          │   │
│    │    └─ Other errors → fail                                            │   │
│    │                                                                       │   │
│    │    FALLBACK CHAIN (if multiple candidates):                           │   │
│    │    └─ Try provider A → fail → try provider B → fail → try C...       │   │
│    └──────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│    ┌──────────────────────────────────────────────────────────────────────┐   │
│    │ 5. HOOKS: AfterLLM                                                    │   │
│    │    └─ Can inspect/modify response or abort                           │   │
│    └──────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│    ┌──────────────────────────────────────────────────────────────────────┐   │
│    │ 6. HANDLE REASONING (Claude extended thinking → debug channel)       │   │
│    └──────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│    ┌──────────────────────────────────────────────────────────────────────┐   │
│    │ 7. CHECK RESPONSE TYPE                                                │   │
│    │                                                                       │   │
│    │    NO TOOL CALLS?                                                     │   │
│    │    └─ finalContent = response.Content → BREAK LOOP                   │   │
│    │                                                                       │   │
│    │    HAS TOOL CALLS?                                                    │   │
│    │    └─ Continue to tool execution...                                  │   │
│    └──────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│    ┌──────────────────────────────────────────────────────────────────────┐   │
│    │ 8. TOOL EXECUTION (for each tool call)                                │   │
│    │                                                                       │   │
│    │    for each toolCall in response.ToolCalls:                           │   │
│    │      ├─ BeforeTool hook (can deny/modify)                            │   │
│    │      ├─ ApproveTool hook (approval gate)                             │   │
│    │      ├─ Log: "🔧 tool_name(args...)"                                  │   │
│    │      │                                                                │   │
│    │      ├─ EXECUTE: agent.Tools.ExecuteWithContext(...)                 │   │
│    │      │    ├─ read_file → read workspace file                         │   │
│    │      │    ├─ exec → run shell command                                │   │
│    │      │    ├─ web_search → search the web                             │   │
│    │      │    ├─ spawn → launch sub-agent                                │   │
│    │      │    └─ etc.                                                    │   │
│    │      │                                                                │   │
│    │      ├─ AfterTool hook                                               │   │
│    │      ├─ Filter sensitive data from result                            │   │
│    │      ├─ Append tool result message to messages[]                     │   │
│    │      └─ Save to session history                                      │   │
│    │                                                                       │   │
│    │    ALL TOOLS HANDLED RESPONSE? (ResponseHandled=true)                 │   │
│    │    └─ Skip follow-up LLM call → BREAK LOOP                           │   │
│    │                                                                       │   │
│    │    Otherwise → CONTINUE LOOP (LLM sees tool results)                  │   │
│    └──────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
└───────────────────────────────────┬────────────────────────────────────────────┘
                                    │
                                    ▼
```

### Finalization Phase

```
┌────────────────────────────────────────────────────────────────────────────────┐
│  TURN FINALIZATION                                                              │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  1. Set final content (or default if empty)                                    │
│                                                                                 │
│  2. Save assistant response to session:                                        │
│     agent.Sessions.AddMessage(sessionKey, "assistant", finalContent)           │
│     agent.Sessions.Save(sessionKey)                                            │
│                                                                                 │
│  3. Maybe summarize (if history exceeds threshold):                            │
│     maybeSummarize(agent, sessionKey, turnScope)                               │
│                                                                                 │
│  4. Emit EventKindTurnEnd                                                      │
│                                                                                 │
│  5. Return turnResult{finalContent, status, followUps}                         │
│                                                                                 │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## Context Building

The ContextBuilder (`pkg/agent/context.go`) assembles the conversation context for each LLM call.

### System Prompt Structure

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                           SYSTEM PROMPT                                         │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────────────────────────────┐                                   │
│  │ STATIC PART (cached)                    │                                   │
│  │  ├─ Identity (picoclaw intro)          │                                   │
│  │  ├─ Workspace path                      │                                   │
│  │  ├─ Important rules                     │                                   │
│  │  ├─ AGENT.md content                    │                                   │
│  │  ├─ SOUL.md content                     │                                   │
│  │  ├─ USER.md content                     │                                   │
│  │  ├─ Skills summary                      │                                   │
│  │  └─ Memory context (MEMORY.md)          │                                   │
│  └─────────────────────────────────────────┘                                   │
│                                                                                 │
│  ┌─────────────────────────────────────────┐                                   │
│  │ DYNAMIC PART (per-request)              │                                   │
│  │  ├─ Current time                        │                                   │
│  │  ├─ Runtime info (OS, arch)             │                                   │
│  │  ├─ Current session (channel, chat)     │                                   │
│  │  └─ Current sender info                 │                                   │
│  └─────────────────────────────────────────┘                                   │
│                                                                                 │
│  ┌─────────────────────────────────────────┐                                   │
│  │ ACTIVE SKILLS (if any)                  │                                   │
│  │  └─ Full SKILL.md content for active    │                                   │
│  └─────────────────────────────────────────┘                                   │
│                                                                                 │
│  ┌─────────────────────────────────────────┐                                   │
│  │ SUMMARY (if history was summarized)     │                                   │
│  │  └─ CONTEXT_SUMMARY: ...                │                                   │
│  └─────────────────────────────────────────┘                                   │
│                                                                                 │
└────────────────────────────────────────────────────────────────────────────────┘
```

### Message Assembly

```go
func (cb *ContextBuilder) BuildMessages(...) []providers.Message {
    messages := []providers.Message{}

    // 1. System message (static + dynamic + summary)
    staticPrompt := cb.BuildSystemPromptWithCache()  // cached
    dynamicCtx := cb.buildDynamicContext(...)        // per-request

    messages = append(messages, providers.Message{
        Role:    "system",
        Content: staticPrompt + dynamicCtx + summary,
    })

    // 2. Conversation history
    messages = append(messages, history...)

    // 3. Current user message
    messages = append(messages, providers.Message{
        Role:    "user",
        Content: currentMessage,
        Media:   media,
    })

    return messages
}
```

---

## Tool Execution

Tools are registered in the ToolRegistry and executed during the turn loop.

### Built-in Tools

| Tool             | Description                     |
| ---------------- | ------------------------------- |
| `read_file`      | Read file contents              |
| `write_file`     | Write/overwrite file            |
| `edit_file`      | String replacement in file      |
| `append_file`    | Append to file                  |
| `list_dir`       | List directory contents         |
| `exec` / `shell` | Execute shell commands          |
| `web_search`     | Search the web                  |
| `fetch_webpage`  | Download webpage content        |
| `send_message`   | Send message to another channel |
| `spawn`          | Launch sub-agent                |
| `spawn_status`   | Check sub-agent status          |
| `cron`           | Schedule recurring tasks        |
| `skill_search`   | Find available skills           |
| `skill_install`  | Install skill from URL          |

### Tool Execution Flow

```
Agent receives tool call from LLM
         │
         ▼
Parse tool name and arguments
         │
         ▼
Look up tool in registry
         │
         ▼
┌─────────────────────────────────┐
│ HOOKS                           │
│  ├─ BeforeTool (can modify)    │
│  └─ ApproveTool (can deny)     │
└─────────────────────────────────┘
         │
         ▼
Check permissions (workspace restrictions)
         │
         ▼
Execute tool
         │
         ▼
┌─────────────────────────────────┐
│ HOOKS                           │
│  └─ AfterTool (can modify)     │
└─────────────────────────────────┘
         │
         ▼
Capture result (text, file, error)
         │
         ▼
Filter sensitive data from result
         │
         ▼
Format result for LLM
         │
         ▼
Return to agent loop
```

### Tool Result Types

```go
type ToolResult struct {
    ForLLM           string   // Content sent to LLM
    ForUser          string   // Content sent directly to user (if any)
    Media            []string // Media refs (files, images)
    IsError          bool     // Whether this is an error result
    ResponseHandled  bool     // If true, skip follow-up LLM call
    Silent           bool     // Don't send ForUser to chat
    Async            bool     // Result will arrive later
}
```

---

## Session Management

Sessions persist conversation history using JSONL files.

### Storage Structure

```
~/.picoclaw/workspace/
├── sessions/
│   ├── cli:default.jsonl        # CLI sessions
│   ├── telegram:123456.jsonl    # Telegram user session
│   └── discord:guild:chan.jsonl # Discord channel session
├── memory/
│   └── MEMORY.md                # Long-term memory
├── AGENT.md                     # Agent instructions
├── SOUL.md                      # Agent personality
└── USER.md                      # User preferences
```

### JSONL Format

Each line is a JSON object representing a message:

```json
{"role":"user","content":"Hello"}
{"role":"assistant","content":"Hi! How can I help?"}
{"role":"user","content":"Read my config file"}
{"role":"assistant","tool_calls":[{"id":"call_1","name":"read_file","arguments":{"path":"config.json"}}]}
{"role":"tool","content":"{ ... file content ... }","tool_call_id":"call_1"}
{"role":"assistant","content":"Here's your config file: ..."}
```

### Summarization

When history exceeds `SummarizeMessageThreshold`:

1. Ask LLM to summarize old messages
2. Store summary separately
3. Truncate old history
4. Keep summary + recent messages

---

## Channel Capabilities

Channels implement optional interfaces for advanced features:

| Interface               | Purpose                      | Example Channels         |
| ----------------------- | ---------------------------- | ------------------------ |
| `TypingCapable`         | Show typing indicator        | Telegram, Discord, Slack |
| `MessageEditor`         | Edit existing messages       | Telegram, Discord        |
| `MessageDeleter`        | Delete messages              | Telegram, Discord        |
| `ReactionCapable`       | Add/remove reactions         | Telegram, Discord        |
| `PlaceholderCapable`    | Send "Thinking…" placeholder | Telegram                 |
| `StreamingCapable`      | Real-time streaming output   | Telegram (forums)        |
| `MessageLengthProvider` | Declare max message length   | All                      |

### Placeholder + Typing Flow

```
Inbound message received
    │
    ├─ StartTyping() → typing... typing... typing...
    ├─ ReactToMessage() → 👀
    └─ SendPlaceholder() → "Thinking... 💭"

    [Agent processes...]

Outbound response ready
    │
    ├─ Stop typing (via recorded stop func)
    ├─ Remove reaction (via recorded undo func)
    └─ EditMessage(placeholder_id, response)  ← placeholder becomes answer
```

---

## CLI vs Gateway Mode

| Aspect              | `picoclaw agent` (CLI) | `picoclaw gateway` (Server)  |
| ------------------- | ---------------------- | ---------------------------- |
| Entry               | `ProcessDirect()`      | `Run()` loop on MessageBus   |
| Channel             | "cli"                  | "telegram", "discord", etc.  |
| Concurrency         | Single request         | Multiple concurrent turns    |
| Typing/Placeholder  | None                   | Platform-specific indicators |
| Response delivery   | `return string`        | `PublishOutbound()` to bus   |
| Session persistence | Same JSONL files       | Same JSONL files             |
| Tools               | Same tool registry     | Same + messaging tools       |

---

## Interactive Mode

```go
// interactiveMode at helpers.go
func interactiveMode(agentLoop *AgentLoop, sessionKey string) {
    rl, _ := readline.NewEx(&readline.Config{
        Prompt:      "🦞 You: ",
        HistoryFile: "/tmp/.picoclaw_history",
    })

    for {
        line, _ := rl.Readline()           // Wait for user input

        if line == "exit" { return }

        response, _ := agentLoop.ProcessDirect(ctx, line, sessionKey)

        fmt.Printf("\n🦞 %s\n\n", response)  // Print response
    }
}
```

Session persistence: Each message in interactive mode builds on the previous conversation history stored in `~/.picoclaw/workspace/sessions/cli:default.jsonl`.
