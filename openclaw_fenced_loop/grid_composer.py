"""Universal grid-governed fenced task store and iterative multi-worker artifact composer.

Implements:
- ParameterBounds, NodeParameter, NodeGrid (with expand_if_saturated)
- GridLedger (represented -> applicable -> proposed -> dispatched -> evaluated -> verified -> promoted
  strictly enforcing maker_id != checker_id)
- FencedTaskStore (SQLite WAL CAS lease_epoch fencing, HMAC fence digest, bounded resume packets,
  and bitemporal verified stage reuse)
- StageSpec & IterativeArtifactComposer (topological multi-stage decomposition into FencedTaskStore,
  independent maker != checker stage verification, bitemporal cache reuse, and deterministic manifest synthesis)

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
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple


DEFAULT_GRID_PATH: Path = Path(__file__).resolve().parent / "default_grid.json"

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
    """Clamping and dynamic boundary expansion rules for an integer_range grid axis."""

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

    def maybe_expand(
        self, empirical_optimum: int, saturation_ratio: float
    ) -> Tuple["ParameterBounds", bool]:
        span = self.high - self.low
        if span <= 0:
            return self, False
        threshold = float(self.low) + float(span) * float(saturation_ratio)
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
    """Single tunable parameter axis inside a universal NodeGrid."""

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
    """Multi-dimensional declarative configuration grid with boundary auto-expansion."""

    grid_id: str
    generation: int
    parameters: Dict[str, NodeParameter]
    constraints: List[Dict[str, str]]
    regime_cells: Dict[str, Dict[str, Any]]
    runtime_constants: Dict[str, Any]
    test_vectors: Dict[str, Any] = field(default_factory=dict)
    expansion_history: List[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def default(cls) -> "NodeGrid":
        return cls.from_json_path(DEFAULT_GRID_PATH)

    @classmethod
    def from_json_path(cls, path: Path) -> "NodeGrid":
        spec = json.loads(Path(path).read_text(encoding="utf-8"))
        params: Dict[str, NodeParameter] = {}
        for raw in spec["parameters"]:
            kind = str(raw["kind"])
            bounds: Optional[ParameterBounds] = None
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
            test_vectors=dict(spec.get("test_vectors", {})),
        )

    def resolve_cell(
        self,
        regime: str = "durable_multi_worker",
        overrides: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        base = dict(self.regime_cells.get(regime) or next(iter(self.regime_cells.values())))
        if overrides:
            for axis_name, axis_val in overrides.items():
                if axis_name in self.parameters and axis_val is not None:
                    base[axis_name] = axis_val
        resolved: Dict[str, Any] = {}
        for axis_name, param in self.parameters.items():
            resolved[axis_name] = param.normalize(base.get(axis_name, param.default))
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

    def expand_if_saturated(self, axis_name: str, empirical_optimum: int) -> bool:
        """Expand an integer_range axis if empirical_optimum saturates its upper boundary."""
        param = self.parameters.get(axis_name)
        if param is None or param.bounds is None:
            return False
        sat_ratio = float(self.runtime_constants["saturation_margin_ratio"])
        old_high = param.bounds.high
        next_bounds, expanded = param.bounds.maybe_expand(int(empirical_optimum), sat_ratio)
        if not expanded:
            return False
        self.parameters[axis_name] = NodeParameter(
            name=param.name,
            kind=param.kind,
            default=param.default,
            choices=param.choices,
            bounds=next_bounds,
        )
        if axis_name in self.regime_cells.get("durable_multi_worker", {}):
            if int(empirical_optimum) > int(self.regime_cells["durable_multi_worker"][axis_name]):
                self.regime_cells["durable_multi_worker"][axis_name] = next_bounds.clamp(
                    int(empirical_optimum)
                )
        self.generation = self.generation + 1
        self.expansion_history.append(
            {
                "axis_name": axis_name,
                "previous_high": old_high,
                "expanded_high": next_bounds.high,
                "empirical_optimum": int(empirical_optimum),
                "generation": self.generation,
            }
        )
        return True

    def auto_expand_axis(self, axis_name: str, empirical_optimum: int) -> bool:
        return self.expand_if_saturated(axis_name, empirical_optimum)


@dataclass
class GridLedger:
    """Configuration cell lifecycle ledger enforcing maker_id != checker_id before promotion."""

    records: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def transition(
        self,
        cell_id: str,
        target_state: str,
        *,
        maker_id: str = "",
        checker_id: str = "",
        quorum_needed: int = 1,
        metrics: Optional[Mapping[str, Any]] = None,
    ) -> str:
        if target_state not in LIFECYCLE_STATES:
            raise ValueError(f"Unknown lifecycle state: {target_state}")
        entry = self.records.setdefault(
            cell_id,
            {
                "state": LIFECYCLE_STATES[0],
                "history": [LIFECYCLE_STATES[0]],
                "maker_id": "",
                "checker_ids": [],
                "verified": False,
                "metrics": {},
            },
        )
        if target_state == "promoted" and not bool(entry.get("verified")):
            raise PermissionError("Cannot promote an unverified configuration cell")
        current_idx = LIFECYCLE_STATES.index(str(entry["state"]))
        target_idx = LIFECYCLE_STATES.index(target_state)
        if target_idx > current_idx + 1:
            raise ValueError(f"Cannot skip lifecycle state from {entry['state']} to {target_state}")

        clean_maker = maker_id.strip()
        if clean_maker:
            if entry["maker_id"] and entry["maker_id"] != clean_maker:
                raise PermissionError("Conflicting maker_id recorded for configuration cell")
            entry["maker_id"] = clean_maker

        if metrics:
            entry["metrics"].update(dict(metrics))

        if target_state == "verified":
            effective_maker = str(entry["maker_id"]).strip()
            clean_checker = checker_id.strip()
            if not effective_maker or not clean_checker:
                raise PermissionError("Both maker_id and checker_id are required for cell verification")
            if effective_maker == clean_checker:
                raise PermissionError("Distinct checker required: maker_id cannot equal checker_id")
            if clean_checker not in entry["checker_ids"]:
                entry["checker_ids"].append(clean_checker)
            if len(entry["checker_ids"]) < int(quorum_needed):
                return str(entry["state"])
            entry["verified"] = True

        if target_state == "promoted" and not bool(entry.get("verified")):
            raise PermissionError("Cannot promote an unverified configuration cell")

        entry["state"] = target_state
        entry["history"].append(target_state)
        return target_state

    def advance_to_evaluated(
        self,
        cell_id: str,
        *,
        maker_id: str,
        metrics: Optional[Mapping[str, Any]] = None,
    ) -> str:
        pre_eval = ("applicable", "proposed", "dispatched", "evaluated")
        entry = self.records.get(cell_id)
        current_state = str(entry["state"]) if entry else LIFECYCLE_STATES[0]
        current_idx = LIFECYCLE_STATES.index(current_state)
        for next_state in pre_eval:
            next_idx = LIFECYCLE_STATES.index(next_state)
            if next_idx > current_idx:
                self.transition(
                    cell_id,
                    next_state,
                    maker_id=maker_id,
                    metrics=metrics if next_state == "evaluated" else None,
                )
        return str(self.records[cell_id]["state"])


class FencedTaskStore:
    """SQLite WAL CAS lease_epoch store with bounded resume packets and bitemporal stage reuse."""

    def __init__(
        self,
        db_path: Path,
        grid: Optional[NodeGrid] = None,
        seed_material: str = "",
    ) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.grid = grid if grid is not None else NodeGrid.default()
        self.ledger = GridLedger()
        hex_len = int(self.grid.runtime_constants["hash_hex_prefix_len"])
        derived_seed = hashlib.sha256(str(self.db_path).encode("utf-8")).hexdigest()[:hex_len]
        self._seed_bytes = (seed_material or derived_seed).encode("utf-8")
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

                CREATE INDEX IF NOT EXISTS idx_verified_lookup
                    ON verified_task_artifacts(task_hash, contract_digest, recorded_at DESC);
                """
            )

    def _compute_fence(self, task_id: str, epoch: int, holder_id: str) -> str:
        hex_len = int(self.grid.runtime_constants["hash_hex_prefix_len"])
        msg = f"{task_id}:{epoch}:{holder_id}".encode("utf-8")
        return hmac.new(self._seed_bytes, msg, hashlib.sha256).hexdigest()[:hex_len]

    def compute_task_hash(
        self, namespace: str, verb: str, payload: Mapping[str, Any]
    ) -> str:
        hex_len = int(self.grid.runtime_constants["hash_hex_prefix_len"])
        canonical = json.dumps(
            {"namespace": namespace, "verb": verb, "payload": dict(payload)},
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
        contract_digest: str = "v1",
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
            regime=regime,
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
        expected = self._compute_fence(task_id, int(lease_epoch), worker_id)
        if not hmac.compare_digest(expected, str(fence_token)):
            return {"ok": False, "reason": "invalid_fence_token"}

        checkpoint_str = json.dumps(dict(checkpoint), sort_keys=True, separators=(",", ":"))
        raw_bytes = len(checkpoint_str.encode("utf-8"))
        expanded = self.grid.expand_if_saturated("resume_packet_byte_cap", raw_bytes)

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
            if checker_id.strip() and checker_id.strip() not in checkers:
                checkers.append(checker_id.strip())

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
                    maker_id.strip(),
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
        regime: str = "durable_multi_worker",
    ) -> Optional[Dict[str, Any]]:
        query_ts = float(as_of if as_of is not None else time.time())
        cell = self.grid.resolve_cell(regime)
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


@dataclass(frozen=True)
class StageSpec:
    """Declarative stage specification for multi-worker iterative composition."""

    stage_id: str
    verb: str
    payload: Mapping[str, Any]
    depends_on: Tuple[str, ...] = ()
    contract_digest: str = "v1"


class IterativeArtifactComposer:
    """Decomposes multi-stage builds into FencedTaskStore tasks and synthesizes a deterministic manifest."""

    def __init__(
        self,
        store: FencedTaskStore,
        *,
        grid: Optional[NodeGrid] = None,
        ledger: Optional[GridLedger] = None,
        regime: str = "durable_multi_worker",
    ) -> None:
        self.store = store
        self.grid = grid if grid is not None else store.grid
        self.ledger = ledger if ledger is not None else store.ledger
        self.regime = regime

    @staticmethod
    def topological_sort(stages: Sequence[StageSpec]) -> List[StageSpec]:
        by_id: Dict[str, StageSpec] = {}
        for spec in stages:
            if spec.stage_id in by_id:
                raise ValueError(f"Duplicate stage_id: {spec.stage_id}")
            by_id[spec.stage_id] = spec

        in_degree: Dict[str, int] = {sid: 0 for sid in by_id}
        dependents: Dict[str, List[str]] = {sid: [] for sid in by_id}
        for spec in stages:
            for dep_id in spec.depends_on:
                if dep_id not in by_id:
                    raise ValueError(
                        f"Stage '{spec.stage_id}' depends on unknown upstream stage '{dep_id}'"
                    )
                in_degree[spec.stage_id] = in_degree[spec.stage_id] + 1
                dependents[dep_id].append(spec.stage_id)

        ready: List[str] = sorted(sid for sid, deg in in_degree.items() if deg == 0)
        ordered: List[StageSpec] = []
        while ready:
            current_id = ready.pop(0)
            ordered.append(by_id[current_id])
            for child_id in sorted(dependents[current_id]):
                in_degree[child_id] = in_degree[child_id] - 1
                if in_degree[child_id] == 0:
                    ready.append(child_id)
                    ready.sort()

        if len(ordered) != len(stages):
            raise ValueError("Cycle detected in stage dependency graph")
        return ordered

    def decompose_build(
        self,
        *,
        build_id: str,
        stages: Sequence[StageSpec],
        namespace: str = "composer",
        now_ts: Optional[float] = None,
    ) -> Dict[str, Any]:
        if len(stages) == 0:
            raise ValueError("stages must be non-empty")
        ordered = self.topological_sort(stages)
        stage_count = len(ordered)
        expanded = self.grid.expand_if_saturated("max_composer_stages", stage_count)
        cell = self.grid.resolve_cell(self.regime)
        max_allowed = int(cell["max_composer_stages"])
        if stage_count > max_allowed:
            raise ValueError(
                f"Stage count {stage_count} exceeds resolved grid bound {max_allowed}"
            )

        enqueued: List[Dict[str, Any]] = []
        for spec in ordered:
            task_id = f"{build_id}:{spec.stage_id}"
            enq = self.store.enqueue_task(
                task_id=task_id,
                namespace=namespace,
                verb=spec.verb,
                payload=spec.payload,
                contract_digest=spec.contract_digest,
                now_ts=now_ts,
            )
            enqueued.append(enq)
        return {
            "build_id": build_id,
            "namespace": namespace,
            "stage_order": [s.stage_id for s in ordered],
            "grid_expanded": expanded,
            "grid_generation": self.grid.generation,
            "enqueued_tasks": enqueued,
        }

    def execute_and_compose(
        self,
        *,
        build_id: str,
        stages: Sequence[StageSpec],
        maker_id: str,
        checker_id: str,
        maker_fn: Callable[[Dict[str, Any]], Mapping[str, Any]],
        checker_fn: Callable[[Dict[str, Any], Mapping[str, Any]], bool],
        namespace: str = "composer",
        cell_id: Optional[str] = None,
        valid_ttl_seconds: Optional[float] = None,
        now_ts: Optional[float] = None,
    ) -> Dict[str, Any]:
        clean_maker = maker_id.strip()
        clean_checker = checker_id.strip()
        if not clean_maker or not clean_checker:
            raise PermissionError("maker_id and checker_id must both be non-empty")
        cell = self.grid.resolve_cell(self.regime)
        if bool(cell["distinct_checker_required"]) and clean_maker == clean_checker:
            raise PermissionError("Distinct checker required: maker_id cannot equal checker_id")

        ts = float(now_ts if now_ts is not None else time.time())
        ttl = float(
            valid_ttl_seconds
            if valid_ttl_seconds is not None
            else self.grid.runtime_constants["default_valid_ttl_seconds"]
        )
        valid_until = ts + ttl
        hex_len = int(self.grid.runtime_constants["hash_hex_prefix_len"])

        plan = self.decompose_build(
            build_id=build_id,
            stages=stages,
            namespace=namespace,
            now_ts=ts,
        )
        ordered = self.topological_sort(stages)
        active_cell_id = cell_id or f"{self.grid.grid_id}:{self.regime}:gen{self.grid.generation}"

        stage_records: Dict[str, Dict[str, Any]] = {}
        reused_count = 0
        executed_count = 0

        for spec in ordered:
            upstream_digests = {
                dep_id: stage_records[dep_id]["stage_digest"] for dep_id in spec.depends_on
            }
            upstream_payloads = {
                dep_id: stage_records[dep_id]["payload"] for dep_id in spec.depends_on
            }
            effective_payload = {
                "stage_id": spec.stage_id,
                "input": dict(spec.payload),
                "upstream_digests": upstream_digests,
            }
            effective_hash = self.store.compute_task_hash(
                namespace, spec.verb, effective_payload
            )
            cached = self.store.lookup_verified_artifact(
                task_hash=effective_hash,
                contract_digest=spec.contract_digest,
                as_of=ts,
                regime=self.regime,
            )

            if cached is not None:
                verified_payload = dict(cached["result"])
                reused_flag = True
                reused_count = reused_count + 1
            else:
                stage_task_id = f"{build_id}:{spec.stage_id}:{effective_hash}"
                self.store.enqueue_task(
                    task_id=stage_task_id,
                    namespace=namespace,
                    verb=spec.verb,
                    payload=effective_payload,
                    contract_digest=spec.contract_digest,
                    now_ts=ts,
                )
                claim = self.store.claim_task(
                    worker_id=clean_maker,
                    task_id=stage_task_id,
                    regime=self.regime,
                    now_ts=ts,
                )
                if not claim.get("ok"):
                    raise RuntimeError(
                        f"Failed to claim fenced stage '{spec.stage_id}': {claim.get('reason')}"
                    )

                stage_ctx = {
                    "build_id": build_id,
                    "stage_id": spec.stage_id,
                    "verb": spec.verb,
                    "payload": dict(spec.payload),
                    "upstream_digests": upstream_digests,
                    "upstream_payloads": upstream_payloads,
                    "lease_epoch": int(claim["lease_epoch"]),
                    "resume_packet": str(claim["resume_packet"]),
                }
                candidate_out = dict(maker_fn(stage_ctx))
                fence = str(claim["fence_token"])
                ckpt = self.store.checkpoint_task(
                    task_id=stage_task_id,
                    worker_id=clean_maker,
                    lease_epoch=int(claim["lease_epoch"]),
                    fence_token=fence,
                    checkpoint={"stage_id": spec.stage_id, "done": True},
                    completed=True,
                    regime=self.regime,
                    now_ts=ts,
                )
                if not ckpt.get("ok"):
                    raise RuntimeError(
                        f"Fenced checkpoint rejected for stage '{spec.stage_id}': {ckpt.get('reason')}"
                    )

                if not bool(checker_fn(stage_ctx, candidate_out)):
                    raise PermissionError(
                        f"Independent checker '{clean_checker}' rejected stage '{spec.stage_id}'"
                    )

                promo = self.store.verify_and_promote_artifact(
                    task_id=stage_task_id,
                    maker_id=clean_maker,
                    checker_id=clean_checker,
                    result_payload=candidate_out,
                    valid_from=ts,
                    valid_to=valid_until,
                    regime=self.regime,
                    now_ts=ts,
                )
                if not promo.get("ok") or not promo.get("promoted_for_reuse"):
                    raise PermissionError(
                        f"Stage '{spec.stage_id}' failed quorum promotion: {promo.get('reason')}"
                    )
                verified_payload = candidate_out
                reused_flag = False
                executed_count = executed_count + 1

            canonical_stage = json.dumps(
                {
                    "stage_id": spec.stage_id,
                    "verb": spec.verb,
                    "contract_digest": spec.contract_digest,
                    "payload": verified_payload,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            stage_digest = hashlib.sha256(canonical_stage.encode("utf-8")).hexdigest()[:hex_len]
            stage_records[spec.stage_id] = {
                "stage_id": spec.stage_id,
                "verb": spec.verb,
                "contract_digest": spec.contract_digest,
                "depends_on": list(spec.depends_on),
                "reused": reused_flag,
                "stage_digest": stage_digest,
                "payload": verified_payload,
            }

        ledger_entry = self.ledger.records.get(active_cell_id)
        if ledger_entry is None or ledger_entry.get("state") != "promoted":
            self.ledger.advance_to_evaluated(
                active_cell_id,
                maker_id=clean_maker,
                metrics={
                    "executed_stages": executed_count,
                    "reused_stages": reused_count,
                },
            )
            self.ledger.transition(
                active_cell_id,
                "verified",
                maker_id=clean_maker,
                checker_id=clean_checker,
                quorum_needed=int(cell["verification_quorum"]),
            )
            self.ledger.transition(active_cell_id, "promoted")

        ordered_entries = [stage_records[s.stage_id] for s in ordered]
        manifest_core = {
            "namespace": namespace,
            "regime": self.regime,
            "stage_order": [s.stage_id for s in ordered],
            "stages": [
                {
                    "stage_id": item["stage_id"],
                    "verb": item["verb"],
                    "contract_digest": item["contract_digest"],
                    "stage_digest": item["stage_digest"],
                    "payload": item["payload"],
                }
                for item in ordered_entries
            ],
        }
        separators = (",", ":") if bool(cell["manifest_compact_json"]) else (", ", ": ")
        canonical_manifest = json.dumps(manifest_core, sort_keys=True, separators=separators)
        manifest_digest = hashlib.sha256(canonical_manifest.encode("utf-8")).hexdigest()[:hex_len]

        return {
            "build_id": build_id,
            "namespace": namespace,
            "regime": self.regime,
            "grid_id": self.grid.grid_id,
            "grid_generation": self.grid.generation,
            "grid_expanded": bool(plan["grid_expanded"]),
            "cell_id": active_cell_id,
            "cell_lifecycle_state": str(self.ledger.records[active_cell_id]["state"]),
            "maker_id": clean_maker,
            "checker_id": clean_checker,
            "stage_order": [s.stage_id for s in ordered],
            "executed_stage_count": executed_count,
            "reused_stage_count": reused_count,
            "stages": ordered_entries,
            "manifest_digest": manifest_digest,
        }
