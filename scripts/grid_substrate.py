"""Grid-governed fenced task loop, verified artifact reuse, and effort calibration.

Universal computational primitives only:
- NodeParameter, ParameterBounds, NodeGrid, GridLedger (with boundary auto-expansion)
- FencedTaskLoopStore (SQLite WAL CAS lease_epoch, cryptographic fence_token,
  compaction-safe resume packets, distinct-checker artifact verification, bitemporal lookup)
- ZeroNetworkEffortCalibrator (local feature calibration + output-token floor guard)

Zero external dependencies; zero outbound network calls; zero hardcoded magic scalars.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple


LIFECYCLE_STATES: Tuple[str, ...] = (
    "represented",
    "applicable",
    "proposed",
    "dispatched",
    "evaluated",
    "verified",
    "promoted",
)


@dataclass(frozen=True)
class ParameterBounds:
    """Clamping and auto-expansion bounds for an integer_range grid axis."""

    low: int
    high: int
    step: int
    expansion_step: int
    hard_ceiling: int

    def clamp(self, raw_value: int) -> int:
        clamped = max(self.low, min(self.high, int(raw_value)))
        if self.step > 1:
            offset = clamped - self.low
            clamped = self.low + (offset // self.step) * self.step
        return max(self.low, min(self.high, clamped))

    def maybe_expand(self, empirical_optimum: int, saturation_ratio: float) -> Tuple["ParameterBounds", bool]:
        span = self.high - self.low
        if span <= 0:
            return self, False
        threshold = self.low + float(span) * float(saturation_ratio)
        if float(empirical_optimum) >= threshold and self.high < self.hard_ceiling:
            expanded_high = min(self.hard_ceiling, self.high + self.expansion_step)
            if expanded_high > self.high:
                return (
                    ParameterBounds(
                        low=self.low,
                        high=expanded_high,
                        step=self.step,
                        expansion_step=self.expansion_step,
                        hard_ceiling=self.hard_ceiling,
                    ),
                    True,
                )
        return self, False


@dataclass(frozen=True)
class NodeParameter:
    """Single tunable dimension in a universal NodeGrid."""

    name: str
    kind: str
    default: Any
    choices: Tuple[str, ...] = ()
    bounds: Optional[ParameterBounds] = None

    def normalize(self, value: Any) -> Any:
        if self.kind == "boolean":
            return bool(value)
        if self.kind == "choice":
            candidate = str(value)
            return candidate if candidate in self.choices else self.default
        if self.kind == "integer_range" and self.bounds is not None:
            return self.bounds.clamp(int(value))
        return value


@dataclass
class NodeGrid:
    """Multi-dimensional declarative configuration grid with cross-axis constraints."""

    grid_id: str
    generation: int
    parameters: Dict[str, NodeParameter]
    constraints: List[Dict[str, str]]
    regime_cells: Dict[str, Dict[str, Any]]
    runtime_constants: Dict[str, Any]

    @classmethod
    def from_json_path(cls, path: Path) -> "NodeGrid":
        spec = json.loads(path.read_text(encoding="utf-8"))
        params: Dict[str, NodeParameter] = {}
        for raw in spec["parameters"]:
            kind = str(raw["kind"])
            bounds = None
            choices: Tuple[str, ...] = ()
            if kind == "integer_range":
                bounds = ParameterBounds(
                    low=int(raw["low"]),
                    high=int(raw["high"]),
                    step=int(raw["step"]),
                    expansion_step=int(raw["expansion_step"]),
                    hard_ceiling=int(raw["hard_ceiling"]),
                )
            elif kind == "choice":
                choices = tuple(str(c) for c in raw.get("choices", []))
            params[str(raw["name"])] = NodeParameter(
                name=str(raw["name"]),
                kind=kind,
                default=raw["default"],
                choices=choices,
                bounds=bounds,
            )
        return cls(
            grid_id=str(spec["grid_id"]),
            generation=int(spec["generation"]),
            parameters=params,
            constraints=list(spec.get("constraints", [])),
            regime_cells=dict(spec.get("regime_cells", {})),
            runtime_constants=dict(spec.get("runtime_constants", {})),
        )

    def resolve_cell(self, regime: str, overrides: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
        base = dict(self.regime_cells.get(regime) or next(iter(self.regime_cells.values())))
        if overrides:
            for key, val in overrides.items():
                if key in self.parameters and val is not None:
                    base[key] = val
        resolved: Dict[str, Any] = {}
        for name, param in self.parameters.items():
            resolved[name] = param.normalize(base.get(name, param.default))
        for rule in self.constraints:
            if rule.get("operator") == "gt":
                left_name = rule["left_param"]
                right_name = rule["right_param"]
                if int(resolved[left_name]) <= int(resolved[right_name]):
                    step_val = 1
                    left_bounds = self.parameters[left_name].bounds
                    if left_bounds is not None:
                        step_val = left_bounds.step
                    resolved[left_name] = int(resolved[right_name]) + step_val
        return resolved

    def auto_expand_axis(self, axis_name: str, empirical_optimum: int) -> bool:
        param = self.parameters.get(axis_name)
        if param is None or param.bounds is None:
            return False
        sat_ratio = float(self.runtime_constants["saturation_margin_ratio"])
        next_bounds, expanded = param.bounds.maybe_expand(empirical_optimum, sat_ratio)
        if not expanded:
            return False
        self.parameters[axis_name] = NodeParameter(
            name=param.name,
            kind=param.kind,
            default=param.default,
            choices=param.choices,
            bounds=next_bounds,
        )
        self.generation = self.generation + 1
        return True


@dataclass
class GridLedger:
    """Tracks configuration cell lifecycle and forbids unverified promotion."""

    records: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def transition(self, cell_id: str, target_state: str, *, verified_ok: bool = False) -> str:
        if target_state not in LIFECYCLE_STATES:
            raise ValueError(f"Unknown lifecycle state: {target_state}")
        entry = self.records.setdefault(
            cell_id,
            {"state": LIFECYCLE_STATES[0], "history": [LIFECYCLE_STATES[0]], "verified": False},
        )
        current_idx = LIFECYCLE_STATES.index(str(entry["state"]))
        target_idx = LIFECYCLE_STATES.index(target_state)
        if target_idx > current_idx + 1:
            raise ValueError(f"Cannot skip lifecycle state from {entry['state']} to {target_state}")
        if target_state == "verified":
            if not verified_ok:
                raise PermissionError("Cannot mark configuration cell verified without checker proof")
            entry["verified"] = True
        if target_state == "promoted" and not bool(entry.get("verified")):
            raise PermissionError("Cannot promote an unverified configuration cell")
        entry["state"] = target_state
        entry["history"].append(target_state)
        return target_state


class ZeroNetworkEffortCalibrator:
    """Deterministic, local-only reasoning-effort and output-budget calibrator."""

    def __init__(self, grid: NodeGrid) -> None:
        self._grid = grid

    def calibrate_request(
        self,
        request: Mapping[str, Any],
        *,
        regime: str = "durable_multi_worker",
        overrides: Optional[Mapping[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        cell = self._grid.resolve_cell(regime, overrides=overrides)
        constants = self._grid.runtime_constants
        updated = dict(request)

        policy = str(cell["effort_policy"])
        user_text = self._extract_latest_user_text(updated)
        stripped = user_text.strip()
        lowered = stripped.lower()
        char_len = len(stripped)

        marker_hits = sum(
            1 for marker in constants["constraint_markers"] if str(marker) in lowered
        )
        structured_requested = bool(updated.get("response_format")) or any(
            str(marker) in lowered for marker in constants["structured_markers"]
        )

        if policy != "adaptive":
            chosen_effort = policy
            effort_source = "static_cell"
        else:
            if char_len <= int(cell["trivial_prompt_char_cap"]) and marker_hits == 0:
                chosen_effort = "minimal"
            elif (
                char_len >= int(cell["high_effort_char_floor"])
                or marker_hits >= int(constants["high_effort_marker_count_floor"])
            ):
                chosen_effort = "high"
            elif (
                char_len >= int(constants["medium_effort_char_floor"])
                or marker_hits >= int(constants["medium_effort_marker_count_floor"])
            ):
                chosen_effort = "medium"
            else:
                chosen_effort = "low"
            effort_source = "grid_adaptive_local"

        if "reasoning_effort" not in updated or updated.get("reasoning_effort") in (None, "", "auto"):
            updated["reasoning_effort"] = chosen_effort

        floor_tokens = int(cell["output_budget_floor_tokens"])
        if structured_requested and updated.get("reasoning_effort") in ("medium", "high", "max"):
            floor_tokens = floor_tokens * int(constants["structured_headroom_multiplier"])

        budget_raised = False
        current_max = updated.get("max_tokens")
        if isinstance(current_max, int) and current_max > 0 and current_max < floor_tokens:
            updated["max_tokens"] = floor_tokens
            budget_raised = True

        diagnostics = {
            "chosen_effort": str(updated.get("reasoning_effort", chosen_effort)),
            "effort_source": effort_source,
            "prompt_chars": char_len,
            "marker_hits": marker_hits,
            "structured_requested": structured_requested,
            "output_floor_applied": budget_raised,
            "effective_max_tokens": updated.get("max_tokens"),
            "grid_generation": self._grid.generation,
        }
        return updated, diagnostics

    @staticmethod
    def _extract_latest_user_text(request: Mapping[str, Any]) -> str:
        messages = request.get("messages")
        if not isinstance(messages, Sequence):
            return ""
        for msg in reversed(list(messages)):
            if isinstance(msg, Mapping) and msg.get("role") == "user":
                content = msg.get("content", "")
                if isinstance(content, str):
                    return content
                if isinstance(content, Sequence):
                    parts = [
                        str(block.get("text", ""))
                        for block in content
                        if isinstance(block, Mapping) and block.get("type") == "text"
                    ]
                    return " ".join(parts)
        return ""


class FencedTaskLoopStore:
    """SQLite WAL durable fenced task queue, resume packet store, and artifact reuse ledger."""

    def __init__(self, db_path: Path, grid: NodeGrid, secret_seed: str = "") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.grid = grid
        self.ledger = GridLedger()
        hex_len = int(grid.runtime_constants["hash_hex_prefix_len"])
        default_seed = hashlib.sha256(str(self.db_path).encode("utf-8")).hexdigest()[:hex_len]
        self._seed = (secret_seed or default_seed).encode("utf-8")
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        timeout_ms = int(self.grid.runtime_constants["sqlite_busy_timeout_ms"])
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute(f"PRAGMA busy_timeout = {timeout_ms}")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS durable_task_leases (
                    task_id TEXT PRIMARY KEY,
                    namespace TEXT NOT NULL,
                    verb TEXT NOT NULL,
                    task_hash TEXT NOT NULL,
                    contract_digest TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    lease_epoch INTEGER NOT NULL DEFAULT 0,
                    holder_id TEXT NOT NULL DEFAULT '',
                    fence_token TEXT NOT NULL DEFAULT '',
                    lease_expires_at REAL NOT NULL DEFAULT 0.0,
                    checkpoint_json TEXT NOT NULL DEFAULT '{}',
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS verified_task_artifacts (
                    artifact_id TEXT PRIMARY KEY,
                    task_hash TEXT NOT NULL,
                    contract_digest TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    maker_id TEXT NOT NULL,
                    checker_ids_json TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    valid_from REAL NOT NULL,
                    valid_to REAL,
                    recorded_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_artifacts_lookup
                    ON verified_task_artifacts(task_hash, contract_digest, recorded_at DESC);

                CREATE TABLE IF NOT EXISTS effort_cost_telemetry (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    chosen_effort TEXT NOT NULL,
                    input_tokens INTEGER NOT NULL,
                    output_tokens INTEGER NOT NULL,
                    recorded_at REAL NOT NULL
                );
                """
            )

    def _compute_fence(self, task_id: str, epoch: int, holder_id: str) -> str:
        hex_len = int(self.grid.runtime_constants["hash_hex_prefix_len"])
        msg = f"{task_id}:{epoch}:{holder_id}".encode("utf-8")
        return hmac.new(self._seed, msg, hashlib.sha256).hexdigest()[:hex_len]

    def compute_task_hash(self, namespace: str, verb: str, payload: Mapping[str, Any]) -> str:
        hex_len = int(self.grid.runtime_constants["hash_hex_prefix_len"])
        canonical = json.dumps(
            {"namespace": namespace, "verb": verb, "payload": payload},
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:hex_len]

    def enqueue_task(
        self,
        *,
        task_id: str,
        namespace: str,
        verb: str,
        payload: Mapping[str, Any],
        contract_digest: str,
        now_ts: Optional[float] = None,
    ) -> Dict[str, Any]:
        ts = float(now_ts if now_ts is not None else time.time())
        task_hash = self.compute_task_hash(namespace, verb, payload)
        payload_str = json.dumps(dict(payload), sort_keys=True, separators=(",", ":"))
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO durable_task_leases (
                    task_id, namespace, verb, task_hash, contract_digest,
                    payload_json, status, lease_epoch, holder_id, fence_token,
                    lease_expires_at, checkpoint_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'queued', 0, '', '', 0.0, '{}', ?)
                ON CONFLICT(task_id) DO NOTHING
                """,
                (task_id, namespace, verb, task_hash, contract_digest, payload_str, ts),
            )
            row = conn.execute(
                "SELECT * FROM durable_task_leases WHERE task_id = ?", (task_id,)
            ).fetchone()
        return {
            "ok": True,
            "task_id": str(row["task_id"]),
            "task_hash": str(row["task_hash"]),
            "contract_digest": str(row["contract_digest"]),
            "status": str(row["status"]),
            "lease_epoch": int(row["lease_epoch"]),
        }

    def claim_task(
        self,
        *,
        worker_id: str,
        namespace: Optional[str] = None,
        task_id: Optional[str] = None,
        regime: str = "durable_multi_worker",
        now_ts: Optional[float] = None,
    ) -> Dict[str, Any]:
        if not worker_id.strip():
            raise ValueError("worker_id must be non-empty")
        ts = float(now_ts if now_ts is not None else time.time())
        cell = self.grid.resolve_cell(regime)
        ttl = float(cell["lease_ttl_seconds"])
        byte_cap = int(cell["resume_packet_byte_cap"])

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if task_id:
                candidate = conn.execute(
                    """
                    SELECT * FROM durable_task_leases
                    WHERE task_id = ?
                      AND (status = 'queued' OR (status = 'leased' AND lease_expires_at <= ?))
                    """,
                    (task_id, ts),
                ).fetchone()
            elif namespace:
                candidate = conn.execute(
                    """
                    SELECT * FROM durable_task_leases
                    WHERE namespace = ?
                      AND (status = 'queued' OR (status = 'leased' AND lease_expires_at <= ?))
                    ORDER BY updated_at ASC LIMIT 1
                    """,
                    (namespace, ts),
                ).fetchone()
            else:
                candidate = conn.execute(
                    """
                    SELECT * FROM durable_task_leases
                    WHERE status = 'queued' OR (status = 'leased' AND lease_expires_at <= ?)
                    ORDER BY updated_at ASC LIMIT 1
                    """,
                    (ts,),
                ).fetchone()

            if candidate is None:
                conn.commit()
                return {"ok": False, "reason": "no_claimable_task"}

            prev_epoch = int(candidate["lease_epoch"])
            next_epoch = prev_epoch + 1
            chosen_id = str(candidate["task_id"])
            fence = self._compute_fence(chosen_id, next_epoch, worker_id)
            expires_at = ts + ttl

            cursor = conn.execute(
                """
                UPDATE durable_task_leases
                SET status = 'leased',
                    lease_epoch = ?,
                    holder_id = ?,
                    fence_token = ?,
                    lease_expires_at = ?,
                    updated_at = ?
                WHERE task_id = ? AND lease_epoch = ?
                """,
                (next_epoch, worker_id, fence, expires_at, ts, chosen_id, prev_epoch),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                return {"ok": False, "reason": "cas_conflict"}
            conn.commit()

        resume_packet = self._build_resume_packet(
            task_id=chosen_id,
            verb=str(candidate["verb"]),
            epoch=next_epoch,
            checkpoint_raw=str(candidate["checkpoint_json"]),
            byte_cap=byte_cap,
        )
        reuse_hit = self.lookup_verified_artifact(
            task_hash=str(candidate["task_hash"]),
            contract_digest=str(candidate["contract_digest"]),
            as_of=ts,
        )
        return {
            "ok": True,
            "task_id": chosen_id,
            "namespace": str(candidate["namespace"]),
            "verb": str(candidate["verb"]),
            "task_hash": str(candidate["task_hash"]),
            "contract_digest": str(candidate["contract_digest"]),
            "lease_epoch": next_epoch,
            "fence_token": fence,
            "lease_expires_at": expires_at,
            "resume_packet": resume_packet,
            "resume_packet_bytes": len(resume_packet.encode("utf-8")),
            "verified_reuse_candidate": reuse_hit,
        }

    def checkpoint_task(
        self,
        *,
        task_id: str,
        worker_id: str,
        lease_epoch: int,
        fence_token: str,
        checkpoint: Mapping[str, Any],
        completed: bool = False,
        regime: str = "durable_multi_worker",
        now_ts: Optional[float] = None,
    ) -> Dict[str, Any]:
        ts = float(now_ts if now_ts is not None else time.time())
        cell = self.grid.resolve_cell(regime)
        expected_fence = self._compute_fence(task_id, int(lease_epoch), worker_id)
        if not hmac.compare_digest(expected_fence, str(fence_token)):
            return {"ok": False, "reason": "invalid_fence_token"}

        checkpoint_str = json.dumps(dict(checkpoint), sort_keys=True, separators=(",", ":"))
        raw_bytes = len(checkpoint_str.encode("utf-8"))
        expanded = self.grid.auto_expand_axis("resume_packet_byte_cap", raw_bytes)

        next_status = "completed_pending_verify" if completed else "leased"
        next_expiry = ts + float(cell["lease_ttl_seconds"])
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE durable_task_leases
                SET checkpoint_json = ?,
                    status = ?,
                    lease_expires_at = ?,
                    updated_at = ?
                WHERE task_id = ? AND lease_epoch = ? AND holder_id = ?
                """,
                (checkpoint_str, next_status, next_expiry, ts, task_id, int(lease_epoch), worker_id),
            )
            if cursor.rowcount != 1:
                return {"ok": False, "reason": "stale_epoch_rejected"}
        return {
            "ok": True,
            "task_id": task_id,
            "lease_epoch": int(lease_epoch),
            "status": next_status,
            "checkpoint_bytes": raw_bytes,
            "grid_boundary_expanded": expanded,
            "grid_generation": self.grid.generation,
        }

    def verify_and_promote_artifact(
        self,
        *,
        task_id: str,
        maker_id: str,
        checker_id: str,
        result_payload: Mapping[str, Any],
        valid_from: Optional[float] = None,
        valid_to: Optional[float] = None,
        regime: str = "durable_multi_worker",
        now_ts: Optional[float] = None,
    ) -> Dict[str, Any]:
        ts = float(now_ts if now_ts is not None else time.time())
        cell = self.grid.resolve_cell(regime)
        if bool(cell["distinct_checker_required"]) and maker_id.strip() == checker_id.strip():
            return {"ok": False, "reason": "distinct_checker_violation"}

        quorum_needed = int(cell["verification_quorum"])
        hex_len = int(self.grid.runtime_constants["hash_hex_prefix_len"])
        with self._connect() as conn:
            task_row = conn.execute(
                "SELECT * FROM durable_task_leases WHERE task_id = ?", (task_id,)
            ).fetchone()
            if task_row is None:
                return {"ok": False, "reason": "unknown_task_id"}

            task_hash = str(task_row["task_hash"])
            contract_digest = str(task_row["contract_digest"])
            namespace = str(task_row["namespace"])
            artifact_id = hashlib.sha256(
                f"{task_id}:{task_hash}:{contract_digest}".encode("utf-8")
            ).hexdigest()[:hex_len]

            existing = conn.execute(
                "SELECT checker_ids_json FROM verified_task_artifacts WHERE artifact_id = ?",
                (artifact_id,),
            ).fetchone()
            checkers: List[str] = (
                json.loads(str(existing["checker_ids_json"])) if existing else []
            )
            if checker_id not in checkers:
                checkers.append(checker_id)

            result_str = json.dumps(dict(result_payload), sort_keys=True, separators=(",", ":"))
            v_from = float(valid_from if valid_from is not None else ts)
            conn.execute(
                """
                INSERT INTO verified_task_artifacts (
                    artifact_id, task_hash, contract_digest, namespace,
                    maker_id, checker_ids_json, result_json,
                    valid_from, valid_to, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(artifact_id) DO UPDATE SET
                    checker_ids_json = excluded.checker_ids_json,
                    result_json = excluded.result_json,
                    recorded_at = excluded.recorded_at
                """,
                (
                    artifact_id,
                    task_hash,
                    contract_digest,
                    namespace,
                    maker_id,
                    json.dumps(checkers),
                    result_str,
                    v_from,
                    valid_to,
                    ts,
                ),
            )
            promoted = len(checkers) >= quorum_needed
            if promoted:
                conn.execute(
                    "UPDATE durable_task_leases SET status = 'verified', updated_at = ? WHERE task_id = ?",
                    (ts, task_id),
                )
        return {
            "ok": True,
            "artifact_id": artifact_id,
            "task_id": task_id,
            "checker_count": len(checkers),
            "quorum_needed": quorum_needed,
            "promoted_for_reuse": promoted,
        }

    def lookup_verified_artifact(
        self,
        *,
        task_hash: str,
        contract_digest: str,
        as_of: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        query_ts = float(as_of if as_of is not None else time.time())
        cell = self.grid.resolve_cell("durable_multi_worker")
        quorum_needed = int(cell["verification_quorum"])
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM verified_task_artifacts
                WHERE task_hash = ?
                  AND contract_digest = ?
                  AND valid_from <= ?
                  AND (valid_to IS NULL OR valid_to > ?)
                ORDER BY recorded_at DESC
                """,
                (task_hash, contract_digest, query_ts, query_ts),
            ).fetchall()
        for row in rows:
            checkers = json.loads(str(row["checker_ids_json"]))
            if len(checkers) >= quorum_needed:
                return {
                    "artifact_id": str(row["artifact_id"]),
                    "task_hash": str(row["task_hash"]),
                    "contract_digest": str(row["contract_digest"]),
                    "maker_id": str(row["maker_id"]),
                    "checker_ids": checkers,
                    "result": json.loads(str(row["result_json"])),
                }
        return None

    def record_effort_telemetry(
        self,
        *,
        session_id: str,
        chosen_effort: str,
        input_tokens: int,
        output_tokens: int,
        now_ts: Optional[float] = None,
    ) -> None:
        ts = float(now_ts if now_ts is not None else time.time())
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO effort_cost_telemetry (
                    session_id, chosen_effort, input_tokens, output_tokens, recorded_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (session_id, chosen_effort, int(input_tokens), int(output_tokens), ts),
            )

    def summarize_status(self) -> Dict[str, Any]:
        with self._connect() as conn:
            by_status = {
                str(r["status"]): int(r["cnt"])
                for r in conn.execute(
                    "SELECT status, COUNT(*) AS cnt FROM durable_task_leases GROUP BY status"
                ).fetchall()
            }
            artifact_count = int(
                conn.execute("SELECT COUNT(*) FROM verified_task_artifacts").fetchone()[0]
            )
            telemetry_count = int(
                conn.execute("SELECT COUNT(*) FROM effort_cost_telemetry").fetchone()[0]
            )
        return {
            "grid_id": self.grid.grid_id,
            "grid_generation": self.grid.generation,
            "tasks_by_status": by_status,
            "verified_artifacts": artifact_count,
            "effort_telemetry_events": telemetry_count,
        }

    @staticmethod
    def _build_resume_packet(
        *, task_id: str, verb: str, epoch: int, checkpoint_raw: str, byte_cap: int
    ) -> str:
        envelope = {
            "task_id": task_id,
            "verb": verb,
            "epoch": epoch,
            "state": json.loads(checkpoint_raw or "{}"),
        }
        encoded = json.dumps(envelope, sort_keys=True, separators=(",", ":"))
        if len(encoded.encode("utf-8")) <= byte_cap:
            return encoded
        compact = {
            "task_id": task_id,
            "verb": verb,
            "epoch": epoch,
            "truncated": True,
        }
        return json.dumps(compact, sort_keys=True, separators=(",", ":"))[:byte_cap]
