---
title: "Part1: Building first data agent"
date: 2026-08-01
slug: "building-your-first-data-agent"
description: "Building your first data agent from scratch: stages, trade-offs, and design decisions"
summary: "Building a reliable data agent from the ground up: starting small with metadata discovery, adding safe query execution, and learning incrementally instead of trying to build perfection on day one."
tags:
  - data-agents
  - data-engineering
categories: ["data-agent"]
draft: false
---

# Building a Data Agent from Scratch (Part 1): Start Small, Learn Fast

Everyone is talking about AI agents, but very few people talk about what it actually takes to build one that people can trust.

Over the next few months, I'm going to build a data agent from scratch and document every decision along the way: the mistakes, the trade-offs, the failures, and the lessons learned.

The goal isn't to build the smartest agent in the world on day one. The goal is much simpler:

**Start small. Learn from real problems. Add capabilities incrementally.**

Instead of trying to build a perfect system, I'll build it one layer at a time:

* Start with a simple agent that understands schemas.
* Add safe query execution.
* Add memory and learning.
* Add evaluation and benchmarks.
* Observe failures and improve the system.
* Scale only after the foundations are solid.

This series is about engineering reliable AI systems, not writing clever prompts.

---

## The Problem

Imagine it's Monday morning.

A VP asks an analyst:

> "How many high-value customers do we have in each region, and what's the trend over the last quarter?"

The analyst has three options.

### Option 1: Write SQL from scratch

* Spend twenty minutes remembering the schema.
* Debug multiple query errors.
* Wait for the query to finish.
* Format the results into a Slack message.

### Option 2: Ask an engineer

* Interrupt someone who is working on something else.
* Wait for them to understand the problem.
* Create another dependency between teams.

### Option 3: Open the BI dashboard

* The dashboard almost answers the question.
* "Close enough" isn't good enough.
* Go back to writing SQL.

I've seen this happen repeatedly.

Talented analysts spend more time formatting queries and remembering table names than actually understanding the data.

That led me to a simple question:

**What if analysts could simply ask questions in plain English?**

---

## The Ideal Experience

Imagine the conversation looked like this:

**Analyst:**

> "Show me the number of high-value customers by region and the trend over the last quarter."

**Agent:**

* Finds the customer and order tables.
* Understands the schema.
* Generates SQL.
* Validates the query.
* Executes it safely.
* Returns the answer with evidence.

**Result:**

> "There are 2,847 high-value customers across all regions, up 12% quarter over quarter."

No schema memorization.

No SQL debugging.

No dependency on engineers.

---

## Why Now?

Recent language models have become surprisingly good at using tools.

Modern models can:

* Understand which tool to call.
* Handle multi-step workflows.
* Chain tool outputs together.
* Ask clarifying questions.
* Explain their reasoning.

But they also fail in many ways.

They can:

* Hallucinate tables that don't exist.
* Generate inefficient queries.
* Scan millions of rows accidentally.
* Return incorrect answers with confidence.

The problem isn't necessarily the model.

The problem is often the system around the model.

I realized that if I designed the tools correctly, I could give an LLM enough power to be useful while keeping it safe.

---

## Stage 1: Metadata Discovery

The first version of the agent is intentionally simple.

It only knows how to inspect the database.

```python
list_tables()

describe_table("customers")

describe_table("orders")
```

The agent cannot invent tables because it only works with real metadata.

Before answering, it must discover:

* Which tables exist.
* Which columns exist.
* How tables are related.

This creates a crucial constraint:

**The database becomes the source of truth.**

---

## Stage 2: Safe Query Execution

Once the agent understands the schema, it needs to generate SQL.

Unfortunately, SQL can be dangerous.

Bad queries can:

* Run forever.
* Consume massive amounts of memory.
* Scan entire warehouses.
* Accidentally modify data.

So every query goes through a validation layer.

```python
def validate_sql(sql: str) -> bool:

    if not is_select_query(sql):
        return False

    if no_limit_clause(sql):
        sql = add_limit(sql, 1000)

    for table in extract_tables(sql):
        if table not in warehouse_schema:
            return False

    for column in extract_columns(sql):
        if column not in schema[column.table]:
            return False

    return True
```

The philosophy is simple:

**Give the agent enough freedom to be useful, but not enough freedom to break things.**

---

## Why I Chose DuckDB

I evaluated several options.

| Option               | Pros                         | Cons                              |
| -------------------- | ---------------------------- | --------------------------------- |
| Production databases | Real-world conditions        | Risky during development          |
| Pandas               | Simple setup                 | Doesn't scale                     |
| DuckDB               | Fast, lightweight, realistic | No distributed compute or prod access controls |

I chose DuckDB because:

* It's a real SQL engine.
* It supports large datasets.
* It's easy to reset and experiment with.
* The lessons transfer to production systems.

The plan is to validate everything locally and later adapt the architecture to production warehouses.

---

## Design Decision: Why Claude?

To be honest, I did not try many models (I'll keep this as a future exersice)

I chose Claude Opus because it has execellent reasoning capabilities, highly reliable in multi-turn tool use and chaining results and is cost effective.

---

## Why Tool Design Matters

One thing became obvious very quickly:

**Tools are product decisions.**

Every tool changes what the agent can and cannot do.

A dangerous tool:

```python
drop_table("customers")
```

A safe tool:

```python
execute_query(sql)
```

with validation, permissions, and evidence.

Every new capability should answer a few questions:

* Can this tool be trusted?
* What permissions does it require?
* Can users verify the output?
* Can it fail safely?

The quality of an agent is often determined more by its tools than by the model itself.

---

## Design Decision: Why Tool Contracts?

I didn't want to just have tools. I wanted to understand them deeply.

Every tool in this system declares a **ToolContract**:

```python
@dataclass
class ToolContract:
    name: str
    description: str
    
    # 1. When can it be called?
    available: bool
    
    # 2. How trustworthy is the output?
    authority: Literal["canonical", "advisory", "stale"]
    
    # 3. What capability does it provide?
    execution_type: Literal["metadata", "live_data", "sql", "publishing"]
    
    # 4. Who can call it and with what constraints?
    permission_scope: str
    
    # 5. What does it return?
    output_format: str
    
    # 6. Can the user audit the work?
    provides_evidence: bool
```

Why? Because when I add a tool, I want to ask:

* Is this tool available in all contexts, or just some?
* Can I trust its output, or is it advisory?
* If something goes wrong, can the user see why?
* Who should be allowed to call this?

This prevents me from building tools that look good but are actually problematic.

---

## The Data Agent Stack

I'm calling this architectural pattern the **Data Agent Stack** because it's layered:

```
Layer 1 (Top):    User Conversation → Natural Language
                        ↓
Layer 2:          Claude → Tool Selection & Reasoning
                        ↓
Layer 3:          Tools → SQL/Metadata/Publishing
                        ↓
Layer 4:          Validation → Safety & Constraints
                        ↓
Layer 5 (Bottom): Database → Ground Truth
```

Each layer is independent, testable, and can be iterated separately. This is important for both reliability and the evolution of the system.

---

## What I've Learned So Far

### 1. Evidence matters more than answers

Users don't only want answers.

They want proof.

They want to know:

* Which tables were used?
* What query was executed?
* What assumptions were made?
* How was the answer generated?

Trust comes from transparency.

---

### 2. Multi-turn conversations are essential

The best agents don't immediately answer.

They ask questions.

Examples:

* "I found three customer tables. Which one do you mean?"
* "Do you mean orders placed or orders shipped?"
* "This query will scan 100 million rows. Should I add a date filter?"

Clarification is intelligence.

---

### 3. Incremental development wins

It's tempting to build memory, evaluation, planning, and execution all at once.

I'm intentionally avoiding that.

Instead:

* Build a small system.
* Test it.
* Observe failures.
* Improve one layer.
* Repeat.

Complex systems emerge from simple systems that work.

---

## The Long-Term Vision

In a year, I'd like analysts to ask questions like:

> "Show customer acquisition cost by marketing channel, flag channels with CAC greater than $50, and compare retention across regions."

And I'd like the agent to:

* Find the right tables.
* Generate multiple SQL queries.
* Explain every step.
* Cache previous results.
* Learn from past interactions.
* Answer follow-up questions.

This is what I am planning to build in next stages. 

---

## What's Next?

The current version can:

* Discover schemas.
* Generate SQL.
* Validate queries.
* Execute safely.

The next milestones are:

* Add memory.
* Build evaluation pipelines.
* Measure accuracy.
* Learn from failures.
* Test with real analysts.
* Optimize performance.

In the next post, I'll dive into the architecture of the tool system and explain how I prevent hallucinations before the model ever generates SQL.

I am planning to build a reliable data agent, one layer at a time.

---

## The Series Ahead

In the coming posts, I'll explore:

1. Build metadata tools.
2. The agent loop and debugging multi-turn conversations
3. Find bugs and lessons from debugging
4. Add different memory layers. 
5. Add evaluation steps.
6. Optimizations
