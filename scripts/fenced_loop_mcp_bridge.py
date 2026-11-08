#!/usr/bin/env python3
"""Dual CLI + JSON-RPC 2.0 Stdio MCP bridge for OpenClaw & Hermes-x-OpenClaw loops.

Shares the exact SQLite WAL durable fenced queue and bitemporal artifact reuse
tables as `fenced-task-loop`, enabling zero-split-brain multi-agent handoffs.
Zero external dependencies; zero magic scalars (all parameters resolved from NodeGrid).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

_SCRIPT_DIR = Path(__file__).resolve().parent
_FALLBACK_PLUGIN_DIR = _SCRIPT_DIR.parent.parent / "fenced-task-loop"
_SUBSTRATE_DIR = _SCRIPT_DIR if (_SCRIPT_DIR / "grid_substrate.py").is_file() else _FALLBACK_PLUGIN_DIR
if str(_SUBSTRATE_DIR) not in sys.path:
    sys.path.insert(0, str(_SUBSTRATE_DIR))

from grid_substrate import FencedTaskLoopStore, NodeGrid  # noqa: E402


MCP_PROTOCOL_VERSION = "2024-11-05"
JSONRPC_VERSION = "2.0"


def _load_store(db_path: Path) -> FencedTaskLoopStore:
    grid = NodeGrid.from_json_path(_SUBSTRATE_DIR / "fenced_loop_grid.json")
    return FencedTaskLoopStore(db_path=db_path, grid=grid)


def _tool_definitions() -> List[Dict[str, Any]]:
    return [
        {
            "name": "loop_task_enqueue",
            "description": "Enqueue an idempotent durable task into the shared SQLite WAL queue.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "task_id": {"type": "string"},
                    "namespace": {"type": "string"},
                    "verb": {"type": "string"},
                    "contract_digest": {"type": "string"},
                    "payload": {"type": "object"},
                },
                "required": ["task_id", "verb", "payload"],
            },
        },
        {
            "name": "loop_task_claim",
            "description": "Atomically claim a task with CAS lease_epoch, fence_token, and compact resume_packet.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "worker_id": {"type": "string"},
                    "namespace": {"type": "string"},
                    "task_id": {"type": "string"},
                    "regime": {"type": "string"},
                },
                "required": ["worker_id"],
            },
        },
        {
            "name": "loop_task_checkpoint",
            "description": "Persist fenced task progress; rejects zombie/stale lease_epoch writers.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "task_id": {"type": "string"},
                    "worker_id": {"type": "string"},
                    "lease_epoch": {"type": "integer"},
                    "fence_token": {"type": "string"},
                    "checkpoint": {"type": "object"},
                    "completed": {"type": "boolean"},
                },
                "required": ["task_id", "worker_id", "lease_epoch", "fence_token", "checkpoint"],
            },
        },
        {
            "name": "loop_artifact_verify_or_reuse",
            "description": "Verify completed work with distinct checker_id != maker_id, or lookup reusable artifact.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "mode": {"type": "string", "enum": ["verify", "lookup"]},
                    "task_id": {"type": "string"},
                    "task_hash": {"type": "string"},
                    "contract_digest": {"type": "string"},
                    "maker_id": {"type": "string"},
                    "checker_id": {"type": "string"},
                    "result": {"type": "object"},
                },
                "required": ["mode"],
            },
        },
    ]


def dispatch_tool_call(store: FencedTaskLoopStore, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    if name == "loop_task_enqueue":
        return store.enqueue_task(
            task_id=str(args["task_id"]),
            namespace=str(args.get("namespace") or "default"),
            verb=str(args["verb"]),
            payload=dict(args.get("payload") or {}),
            contract_digest=str(args.get("contract_digest") or "v1"),
        )
    if name == "loop_task_claim":
        return store.claim_task(
            worker_id=str(args["worker_id"]),
            namespace=args.get("namespace"),
            task_id=args.get("task_id"),
            regime=str(args.get("regime") or "durable_multi_worker"),
        )
    if name == "loop_task_checkpoint":
        return store.checkpoint_task(
            task_id=str(args["task_id"]),
            worker_id=str(args["worker_id"]),
            lease_epoch=int(args["lease_epoch"]),
            fence_token=str(args["fence_token"]),
            checkpoint=dict(args.get("checkpoint") or {}),
            completed=bool(args.get("completed", False)),
        )
    if name == "loop_artifact_verify_or_reuse":
        if str(args.get("mode")) == "verify":
            return store.verify_and_promote_artifact(
                task_id=str(args["task_id"]),
                maker_id=str(args["maker_id"]),
                checker_id=str(args["checker_id"]),
                result_payload=dict(args.get("result") or {}),
            )
        found = store.lookup_verified_artifact(
            task_hash=str(args["task_hash"]),
            contract_digest=str(args.get("contract_digest") or "v1"),
        )
        return {"ok": found is not None, "artifact": found}
    raise ValueError(f"Unknown MCP tool: {name}")


def run_mcp_stdio(store: FencedTaskLoopStore) -> int:
    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        msg = json.loads(line)
        req_id = msg.get("id")
        method = str(msg.get("method") or "")
        params = msg.get("params") or {}

        if method == "initialize":
            reply = {
                "jsonrpc": JSONRPC_VERSION,
                "id": req_id,
                "result": {
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "fenced-loop-mcp-bridge", "version": "0.1.0"},
                },
            }
        elif method == "tools/list":
            reply = {
                "jsonrpc": JSONRPC_VERSION,
                "id": req_id,
                "result": {"tools": _tool_definitions()},
            }
        elif method == "tools/call":
            tool_name = str(params.get("name") or "")
            tool_args = dict(params.get("arguments") or {})
            payload = dispatch_tool_call(store, tool_name, tool_args)
            reply = {
                "jsonrpc": JSONRPC_VERSION,
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": json.dumps(payload, sort_keys=True)}],
                    "structuredContent": payload,
                },
            }
        elif req_id is None:
            continue
        else:
            reply = {
                "jsonrpc": JSONRPC_VERSION,
                "id": req_id,
                "error": {"code": -1, "message": f"Unsupported method: {method}"},
            }
        sys.stdout.write(json.dumps(reply, sort_keys=True) + "\n")
        sys.stdout.flush()
    return 0


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fenced Loop CLI + Stdio MCP Bridge")
    parser.add_argument("--db", default="./.fenced_loop.sqlite3", help="SQLite WAL ledger path")
    parser.add_argument("--mcp-stdio", action="store_true", help="Serve JSON-RPC 2.0 MCP over stdio")
    sub = parser.add_subparsers(dest="cmd")

    p_enq = sub.add_parser("enqueue")
    p_enq.add_argument("--task-id", required=True)
    p_enq.add_argument("--namespace", default="default")
    p_enq.add_argument("--verb", required=True)
    p_enq.add_argument("--contract-digest", default="v1")
    p_enq.add_argument("--payload-json", default="{}")

    p_clm = sub.add_parser("claim")
    p_clm.add_argument("--worker-id", required=True)
    p_clm.add_argument("--namespace", default=None)
    p_clm.add_argument("--task-id", default=None)

    p_stat = sub.add_parser("status")

    ns = parser.parse_args(argv)
    store = _load_store(Path(ns.db))
    if ns.mcp_stdio:
        return run_mcp_stdio(store)
    if ns.cmd == "enqueue":
        out = store.enqueue_task(
            task_id=ns.task_id,
            namespace=ns.namespace,
            verb=ns.verb,
            payload=json.loads(ns.payload_json),
            contract_digest=ns.contract_digest,
        )
    elif ns.cmd == "claim":
        out = store.claim_task(worker_id=ns.worker_id, namespace=ns.namespace, task_id=ns.task_id)
    else:
        out = store.summarize_status()
    sys.stdout.write(json.dumps(out, indent=True if False else None, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
