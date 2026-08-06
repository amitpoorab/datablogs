+++
title = "Adding memory to the data agent"
date = 2026-08-06
slug = "adding-memory-to-the-data-agent"
description = "Adding session memory and learned notes to the data agent"
summary = "Building session persistence and agent-directed memory: storing conversations, extracting learned insights as notes, and injecting them into every future interaction."
tags = ["data-agents", "data-engineering"]
categories = ["data-agent"]
draft = false
+++


# Part 2: Building Memory Continuous Learning

## The Challenge

After Stage 1 and Stage 2 were working — metadata discovery, safe query execution — I hit a wall that had nothing to do with SQL.

Every call to `run_agent_loop()` looked like this under the hood:

```python
# agent/main.py, before this iteration
while True:
    prompt = input("You: ").strip()
    if prompt.lower() == "exit": break
    run_agent_loop(prompt)  # Fresh conversation, every time
    print()
```

And inside `run_agent_loop()`, `messages` started life as an empty list on every single call. So a real interaction looked like this:

```
You: How many orders do we have?
Agent: 1,500,000 total orders.

You: Break that down by year.
Agent: Break down what by year? I don't have a number in front of me.
```

This happens because the model does not store the context of previous conversations. It had no idea what happened in the previous query. In every session it figures out some details about the schema, but it does not keep track of them. The work it did earlier is gone.

---

## There were primaity Two Defects

Digging in, memory wasn't blocked by one bug — it was blocked by two, and they compounded.

### Defect 1: SDK objects don't serialize

`agent/loop.py` was doing this:

```python
messages.append({"role": "assistant", "content": response.content})
```

`response.content` is a list of Anthropic SDK objects — TextBlock, ToolUseBlock. These are pydantic models, not plain dicts. So the moment I tried to save messages to disk, or even json.dumps() it for a debug log, it broke.

### Learning #1

Serialization is not something you fix later at the persistence layer. Once your in-memory state stops being JSON-serializable, that debt shows up in everything downstream — testing, debugging, and persistence.

### Learning #2

State belongs at the CLI/REPL layer, not inside the loop. The loop should not know anything about sessions. It should take a session and a message history as input, and return the updated versions.

---

## What I Built

### 1. Fixed the serialization defect

One line, `agent/loop.py`:

```python
# Before
messages.append({"role": "assistant", "content": response.content})

# After
content_dicts = [b.model_dump() for b in response.content]
messages.append({"role": "assistant", "content": content_dicts})
```

`.model_dump()` is exactly what Anthropic's SDK provides for this. Now `messages` is a plain `list[dict]`.

### 2. A three-module persistence layer

I built three new modules to handle storage, backed by a new `agent_memory.duckdb` file alongside the existing `warehouse.duckdb`:

**`agent/store.py`** — shared connection + idempotent schema init
```python
def get_conn(): ...          # connects to agent_memory.duckdb
def init_schema(): ...        # CREATE TABLE IF NOT EXISTS, safe to call every startup
```

**`agent/session.py`** — conversation persistence
```python
def create_session() -> str: ...
def save_turn(session_id, turn_index, role, content): ...
def load_session(session_id) -> list[dict]: ...
def list_sessions() -> list[dict]: ...
```

**`agent/notes.py`** — learned memory
```python
def write_note(category, content, session_id, ttl_days=None) -> str: ...
def get_active_notes(category=None) -> list[dict]: ...
def format_notes_for_prompt(notes) -> str: ...
```

Three tables back this: `sessions`, `messages` (JSON blob per turn, keyed by `session_id` + `turn_index`), and `notes` (category, content, optional expiry, provenance via `session_id`).

### Learning #3

`init_schema()` uses `CREATE TABLE IF NOT EXISTS` and runs on every startup. No migrations, no versioning — if the tables exist, it's a no-op. For a project this size, that simplicity is worth far more than a "proper" migration system.

### 3. A stateful REPL

```python
# agent/main.py, after
session_id = None
messages = None

while True:
    prompt = input("You: ").strip()
    if prompt.lower() == "exit": break
    if not prompt: continue

    messages, session_id = run_agent_loop(prompt, session_id, messages)
    print()
```

Now every prompt appends to the *same* `messages` list, and every turn saves to the *same* `session_id`. This one change is why "break that down by year" suddenly means something.

### 4. Notes injected automatically into every system prompt

```python
active_notes = notes_module.get_active_notes()
if active_notes:
    notes_block = notes_module.format_notes_for_prompt(active_notes)
    system_prompt += "\n\n" + notes_block
```

The agent calls `write_note` when it discovers something worth remembering, a schema quirk, a query pattern, a business definition — and every *future* session sees that note in its system prompt automatically. No retrieval step, no explicit "check your memory" tool call required.

### Learning #4

I originally designed `write_note` as something the agent could *choose* to invoke when it felt like it. The real value turned out to be the other half — notes are *unconditionally* injected into every future system prompt. The agent doesn't have to remember to ask for its own notes; it just sees them, every time. That's simpler than RAG, and it mirrors what MemGPT calls agent-directed memory management and what Reflexion calls an episodic buffer of self-written reflections (more in Further Reading).

The tradeoff:

✅ Simple, guaranteed the model sees it
❌ Context grows as you accumulate notes (but for a data agent, probably manageable)

I'll fix this in the next stages if this become a bottleneck. 

### 5. New CLI flags

```bash
python agent/main.py --list-sessions        # see all saved sessions
python agent/main.py --resume <session_id>  # pick up a prior conversation
```

---

## Proof It Actually Works

Here's a real transcript from this project, after all the above landed:

```
User: How many orders do we have?
Agent: 1,500,000 total orders.

User: Break that down by year.
Agent: (remembers the prior context) Orders by year:
   | Year | Orders |
   | 1992 | 227,089 |
   | ... | ... |

User: What about 1898?
Agent: There's no 1898 in the results — the data only spans 1992-01-01 to 1998-08-02...
```

That last answer involved **zero tool calls**. The agent didn't re-query the database — it read the year-range fact straight out of the conversation it was already having and answered directly. That's the entire point: memory that's just *there*, not memory you have to go fetch.

---

## Why DuckDB Again

Same reasoning as Stage1: handles JSON columns natively, fast enough for single-process use, and I can inspect it directly from the CLI:

```bash
duckdb agent_memory.duckdb "SELECT * FROM notes"
```

### Learning #5

The single-writer constraint (DuckDB allows one read-write connection at a time) is a real limitation I'm deliberately not solving yet. Fine for a CLI used by one person at a time; would need a file lock or a different database the moment anything parallelizes against it.

---

## What I Deliberately Left Unfinished

A few things I knew were rough when I shipped this, on purpose:

- **Notes are global** — nothing scopes them by session or user. Anyone's notes are everyone's notes.
- **Every note gets injected, always** — no filtering by relevance to the current question.
- **`write_note` output is trusted unconditionally** — no verification that what the agent wrote down is actually true.

I noticed these while building, but shipping the working version first mattered more than solving problems I hadn't proven were real yet. Better to ship, then measure whether they actually bite — which is exactly what I did next.

---

## Testing

Verified directly, no fuzzy grading needed:
1. Schema init is idempotent (safe to call every startup)
2. Session CRUD (create, load, list) — all round-trip correctly
3. Message persistence — confirmed across multiple sessions
4. Notes lifecycle — write, retrieve, category filter, prompt formatting
5. `write_note` tool dispatch correctly threads `session_id`
6. Existing regression tests still pass with the new `(messages, session_id)` return type


## References

This design leans on a few established patterns from agent-memory research:

- **MemGPT: Towards LLMs as Operating Systems** (Packer et al., UC Berkeley, arXiv:2310.08560, 2023) — the `write_note` pattern mirrors MemGPT's core idea of letting the agent manage its own memory tiers via function calls, rather than memory being something done *to* it.
- **Reflexion: Language Agents with Verbal Reinforcement Learning** (Shinn et al., NeurIPS 2023, arXiv:2303.11366) — agents that verbally reflect on outcomes and store those reflections for future attempts. The "discover a schema quirk, write it down, never repeat the mistake" pattern is exactly this.
- **Generative Agents: Interactive Simulacra of Human Behavior** (Park et al., UIST 2023) — the foundational "agents need persistent memory" paper; introduced the memory-stream architecture this project's notes system loosely follows.
- **Mem0: Building Production-Ready AI Agents with Scalable Long-Term Memory** (arXiv:2504.19413) — a production system independently converged on the same short-term/long-term split used here.


## What's Next?

Shipping this felt great. The "what about 1898?" transcript above is satisfying to watch. But a question kept nagging me: is this actually good, or does it just feel good?

Right now I have no way to answer that. I can make more improvements, but how would I know if they are working? That is what the next post is about — evaluating the agent's performance.

---

## 💬 Discussion

Have you built session persistence or agent-directed memory into an LLM tool? Curious what tradeoffs you hit, especially around when to inject context automatically versus retrieve it on demand.

---

**Tags**: `ai-agents`, `memory-systems`, `duckdb`, `agentic-ai`, `session-persistence`

**Reading time**: 7 min

**Last updated**: August 2, 2026
