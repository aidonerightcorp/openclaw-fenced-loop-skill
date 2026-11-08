---
name: fenced-loop-executor
description: Claim, fence-checkpoint, and verify durable tasks across multi-agent and Hermes-x-OpenClaw workflows with CAS lease epochs and bitemporal artifact reuse.
version: 0.1.0
author: aidonerightcorp
tags:
  - durable-queue
  - lease-fencing
  - multi-agent
  - mcp
  - verification
metadata:
  openclaw:
    requires:
      bins:
        - python3
---

# Fenced Loop Executor (`fenced-loop-executor`)

Use this skill whenever executing multi-step channel, browser, or code actions that must survive retries, gateway reconnects, or multi-agent handoffs (including **Hermes Brain × OpenClaw Hands** pipelines) without duplicate side effects or zombie overwrites.

## Core Protocol (`4` Deterministic Steps)

1. **Check Reuse First (`lookup`)** — Query `fenced_loop_mcp_bridge.py` for an already verified artifact matching `(task_hash, contract_digest)` before invoking expensive tools or external APIs.
2. **Atomic CAS Claim (`claim`)** — Acquire `(lease_epoch, fence_token, resume_packet)` for your `worker_id`. Resume from `resume_packet` instead of replaying completed side effects.
3. **Fenced Checkpoint (`checkpoint`)** — Persist progress with `(task_id, worker_id, lease_epoch, fence_token)`. Abort immediately if `stale_epoch_rejected` is returned (another worker now owns a newer epoch).
4. **Distinct-Checker Promotion (`verify`)** — Never self-certify: pass a distinct `checker_id != maker_id` after deterministic verification so future runs hit the verified artifact cache.

## CLI & Stdio MCP Invocation

```bash
# Direct CLI action against shared SQLite WAL ledger:
python3 scripts/fenced_loop_mcp_bridge.py --db "${FENCED_LOOP_DB:-./.fenced_loop.sqlite3}" claim --worker-id openclaw-worker-1

# Or mount as a JSON-RPC 2.0 Stdio MCP server in OpenClaw / Hermes mcp_servers:
python3 scripts/fenced_loop_mcp_bridge.py --db "${FENCED_LOOP_DB:-./.fenced_loop.sqlite3}" --mcp-stdio
```
