# `openclaw-fenced-loop-skill` — Fenced Task Leases, `NodeGrid` Governance & Iterative Multi-Worker Composer

`openclaw-fenced-loop-skill` (`openclaw_fenced_loop` + `fenced-loop-executor`) provides a **100% Python standard-library + SQLite WAL** fenced task execution engine, declarative configuration grid (`NodeGrid` + `GridLedger`), **Iterative Multi-Worker Artifact Composer (`IterativeArtifactComposer`)**, and **JSON-RPC 2.0 Stdio MCP bridge** for **OpenClaw** (`ClawHub`) and **Hermes Brain × OpenClaw Hands** pipelines.

## Core Primitives

- **Atomic CAS Lease-Epoch Fencing (`FencedTaskStore`)**: Increments `lease_epoch` (`BEGIN IMMEDIATE` + compare-and-swap) and mints an HMAC-SHA256 `fence_token`. Delayed checkpoints from stalled or reconnected gateway workers (`epoch < current`) fail closed with `stale_epoch_rejected`.
- **Compaction-Safe Resume Packets**: Every claim emits a bounded JSON resume packet (`<= 320` bytes by default, governed by `default_grid.json` with automatic boundary expansion) so channel and code workers resume cleanly without replaying completed side effects.
- **Declarative `NodeGrid` + `ParameterBounds` (`expand_if_saturated`)**: All behavioral capacities, lease TTLs, stage limits, and verification quorums live in externalized JSON (`openclaw_fenced_loop/default_grid.json`) with cross-parameter constraints and automatic axis expansion when empirical optima saturate initial grid bounds.
- **Verification-Gated `GridLedger` (`maker_id != checker_id`)**: Tracks configuration cell transitions (`represented -> applicable -> proposed -> dispatched -> evaluated -> verified -> promoted`) and strictly blocks self-certification (`maker_id == checker_id`) or unverified promotion.
- **`IterativeArtifactComposer` (Multi-Stage DAG Decomposition & Deterministic Manifests)**: Topologically validates and decomposes multi-stage builds (`StageSpec`) into fenced tasks in `FencedTaskStore`, binds downstream inputs to upstream stage digests, reuses unexpired bitemporal cached stages at zero execution cost, verifies new stages via an independent checker (`maker_id != checker_id`), and synthesizes a deterministic manifest digest.

## Python API (`openclaw_fenced_loop`)

```python
from pathlib import Path
from openclaw_fenced_loop import (
    FencedTaskStore,
    IterativeArtifactComposer,
    NodeGrid,
    StageSpec,
)

grid = NodeGrid.default()
store = FencedTaskStore(db_path=Path("./.fenced_composer.sqlite3"), grid=grid)
composer = IterativeArtifactComposer(store, grid=grid, regime="durable_multi_worker")

stages = [
    StageSpec(stage_id="schema_init", verb="emit_schema", payload={"rev": "v1"}),
    StageSpec(
        stage_id="compile_core",
        verb="compile_unit",
        payload={"module": "router"},
        depends_on=("schema_init",),
    ),
]

manifest = composer.execute_and_compose(
    build_id="release_01",
    stages=stages,
    maker_id="openclaw-maker-1",
    checker_id="hermes-checker-2",
    maker_fn=lambda ctx: {"built": ctx["stage_id"], "deps": ctx["upstream_digests"]},
    checker_fn=lambda ctx, out: out.get("built") == ctx["stage_id"],
)
print(manifest["manifest_digest"], manifest["cell_lifecycle_state"])
```

## OpenClaw Skill & Stdio MCP Invocation

```bash
# Install in OpenClaw / ClawHub workspace:
openclaw skills install aidonerightcorp/openclaw-fenced-loop-skill

# Direct CLI action against a shared SQLite WAL ledger:
python3 scripts/fenced_loop_mcp_bridge.py --db ./.fenced_loop.sqlite3 enqueue \
  --task-id task-01 --verb execute --payload-json '{"step":"sync"}'

python3 scripts/fenced_loop_mcp_bridge.py --db ./.fenced_loop.sqlite3 claim \
  --worker-id openclaw-worker-1

# Mount as a JSON-RPC 2.0 Stdio MCP server in OpenClaw or Hermes mcp_servers:
python3 scripts/fenced_loop_mcp_bridge.py --db ./.fenced_loop.sqlite3 --mcp-stdio
```

## Verification & Conformance Gates

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -p no:cacheprovider -q
```

- Zero external runtime dependencies (`sqlite3`, `hmac`, `hashlib`, `json`, `argparse` stdlib only).
- Mechanical `ZeroHardcodedValuesAstGate` + `DomainAgnosticNamingGate` enforced in `tests/test_grid_composer.py`.
- Zero outbound network calls, zero telemetry, zero environment secret reads.
