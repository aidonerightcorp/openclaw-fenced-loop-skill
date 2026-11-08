# `openclaw-fenced-loop-skill` — Fenced Task Leases & Verified Reuse Bridge for OpenClaw & MCP

`openclaw-fenced-loop-skill` (`fenced-loop-executor`) provides a **100% Python standard-library + SQLite WAL** fenced task execution protocol and **JSON-RPC 2.0 Stdio MCP bridge** for **OpenClaw** skills (`ClawHub`) and **Hermes Brain × OpenClaw Hands** multi-agent workflows:

- **Atomic CAS Lease-Epoch Fencing (`claim`, `checkpoint`)**: Increments `lease_epoch` (`BEGIN IMMEDIATE` + compare-and-swap) and mints an HMAC-SHA256 `fence_token`. Delayed checkpoints from stalled/reconnected gateway workers (`epoch < current`) fail closed with `stale_epoch_rejected`.
- **Compaction-Safe Resume Packet**: Every claim emits a bounded JSON resume packet (`<= 320` bytes by default, governed by `scripts/fenced_loop_grid.json` with automatic boundary expansion) so channel/browser workers resume cleanly without replaying completed side effects.
- **Distinct-Checker (`maker != checker`) Artifact Reuse (`verify`, `lookup`)**: Blocks self-certification (`maker_id == checker_id`) and caches verified outputs keyed by `(task_hash, contract_digest)` with bitemporal `valid_from` / `valid_to` bounds.

## Installation & Usage

```bash
# Install in OpenClaw / ClawHub workspace:
openclaw skills install aidonerightcorp/openclaw-fenced-loop-skill

# Direct CLI action against a shared SQLite WAL ledger:
python3 scripts/fenced_loop_mcp_bridge.py --db ./.fenced_loop.sqlite3 enqueue \
  --task-id task-01 --verb execute --payload '{"step":"sync"}'

python3 scripts/fenced_loop_mcp_bridge.py --db ./.fenced_loop.sqlite3 claim \
  --worker-id openclaw-worker-1

# Mount as a JSON-RPC 2.0 Stdio MCP server in OpenClaw or Hermes mcp_servers:
python3 scripts/fenced_loop_mcp_bridge.py --db ./.fenced_loop.sqlite3 --mcp-stdio
```

## Security & Privacy

- Zero external Python dependencies (stdlib `sqlite3`, `hmac`, `hashlib`, `json`, `argparse` only).
- Zero outbound network calls, zero telemetry, zero environment secret reads.
