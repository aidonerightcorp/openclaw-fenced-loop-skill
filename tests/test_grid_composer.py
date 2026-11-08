"""Verification suite for openclaw_fenced_loop grid governance and iterative artifact composer.

Enforces:
1. ZeroHardcodedValuesAstGate across every Python file in the repository
2. DomainAgnosticNamingGate across filenames, JSON schemas, and AST definitions
3. ParameterBounds clamping and NodeGrid.expand_if_saturated boundary auto-expansion
4. GridLedger lifecycle ordering and strict maker_id != checker_id verification quorum
5. FencedTaskStore CAS lease_epoch zombie-worker fencing and bounded resume packets
6. IterativeArtifactComposer DAG validation, stage saturation auto-expansion,
   bitemporal stage cache reuse, and deterministic manifest synthesis
"""

from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Mapping, Set

import pytest

from openclaw_fenced_loop import (
    FencedTaskStore,
    GridLedger,
    IterativeArtifactComposer,
    LIFECYCLE_STATES,
    NodeGrid,
    StageSpec,
)

REPO_ROOT: Path = Path(__file__).resolve().parents[1]
BRIDGE_SCRIPT: Path = REPO_ROOT / "scripts" / "fenced_loop_mcp_bridge.py"

ALLOWED_NUMERIC_IDENTITIES: Set[float] = {0, 1, -1, 0.0, 1.0, -1.0}
FORBIDDEN_INDUSTRY_TOKENS: Set[str] = {
    "real_estate",
    "realestate",
    "mortgage",
    "parcel",
    "healthcare",
    "hospitality",
    "ecommerce",
    "fintech",
}


def test_zero_hardcoded_values_ast_gate_all_python_files() -> None:
    """Enforce zero magic numeric scalars across every Python file in the repository."""
    py_files = sorted(
        p
        for p in REPO_ROOT.rglob("*.py")
        if not any(part in p.parts for part in (".git", "__pycache__", ".pytest_cache"))
    )
    assert len(py_files) >= 1
    violations: List[str] = []
    for py_file in py_files:
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, (int, float))
                and not isinstance(node.value, bool)
            ):
                if float(node.value) not in ALLOWED_NUMERIC_IDENTITIES:
                    violations.append(f"{py_file.relative_to(REPO_ROOT)}:{node.lineno} literal={node.value!r}")
            elif isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
                if (
                    isinstance(node.operand, ast.Constant)
                    and isinstance(node.operand.value, (int, float))
                    and not isinstance(node.operand.value, bool)
                ):
                    if -float(node.operand.value) not in ALLOWED_NUMERIC_IDENTITIES:
                        violations.append(
                            f"{py_file.relative_to(REPO_ROOT)}:{node.lineno} literal=-{node.operand.value!r}"
                        )
    assert violations == [], f"ZeroHardcodedValuesAstGate violations: {violations}"


def test_domain_agnostic_naming_gate_all_repo_files() -> None:
    """Enforce universal domain-agnostic naming across files, JSON schemas, and AST symbols."""
    violations: List[str] = []
    for path in sorted(REPO_ROOT.rglob("*")):
        if any(part in path.parts for part in (".git", "__pycache__", ".pytest_cache")):
            continue
        lname = path.name.lower()
        for token in FORBIDDEN_INDUSTRY_TOKENS:
            if token in lname:
                violations.append(f"path:{path.relative_to(REPO_ROOT)}")
        if path.is_file() and path.suffix == ".json":
            doc = json.loads(path.read_text(encoding="utf-8"))
            schema_id = str(doc.get("schema_id") or "").lower()
            for token in FORBIDDEN_INDUSTRY_TOKENS:
                if token in schema_id:
                    violations.append(f"schema:{path.relative_to(REPO_ROOT)}:{schema_id}")
        if path.is_file() and path.suffix == ".py":
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    sym_lower = node.name.lower()
                    for token in FORBIDDEN_INDUSTRY_TOKENS:
                        if token in sym_lower:
                            violations.append(f"symbol:{path.name}:{node.name}")
    assert violations == [], f"DomainAgnosticNamingGate violations: {violations}"


def test_parameter_bounds_clamping_and_grid_expand_if_saturated() -> None:
    """Verify ParameterBounds clamping, cross-axis constraints, and boundary auto-expansion."""
    grid = NodeGrid.default()
    tv = grid.test_vectors
    bounds = grid.parameters["resume_packet_byte_cap"].bounds
    assert bounds is not None
    assert bounds.clamp(int(tv["clamp_below_low"])) == bounds.low
    assert bounds.clamp(int(tv["clamp_above_high"])) == bounds.high

    # Enforce cross-parameter constraint: resume_packet_byte_cap > max_composer_stages
    resolved = grid.resolve_cell(
        "durable_multi_worker",
        overrides={
            "max_composer_stages": bounds.low,
            "resume_packet_byte_cap": bounds.low,
        },
    )
    assert int(resolved["resume_packet_byte_cap"]) >= int(resolved["max_composer_stages"])

    # Boundary auto-expansion when empirical optimum hits upper saturation zone
    old_gen = grid.generation
    stage_bounds = grid.parameters["max_composer_stages"].bounds
    assert stage_bounds is not None
    old_high = stage_bounds.high
    assert grid.expand_if_saturated("max_composer_stages", stage_bounds.low) is False
    assert grid.generation == old_gen

    assert grid.expand_if_saturated("max_composer_stages", old_high) is True
    assert grid.generation == old_gen + 1
    expanded_bounds = grid.parameters["max_composer_stages"].bounds
    assert expanded_bounds is not None
    assert expanded_bounds.high == old_high + stage_bounds.expansion_step
    assert len(grid.expansion_history) == 1


def test_grid_ledger_lifecycle_and_maker_not_equal_checker_quorum() -> None:
    """Verify sequential lifecycle states and strict maker_id != checker_id enforcement."""
    grid = NodeGrid.default()
    quorum_two = int(grid.test_vectors["quorum_two"])
    ledger = GridLedger()

    with pytest.raises(ValueError):
        ledger.transition("cell_skip", "evaluated", maker_id="maker_alpha")

    state = ledger.advance_to_evaluated("cell_a", maker_id="maker_alpha", metrics={"loss": 0.0})
    assert state == "evaluated"

    # Promoting before verified must fail closed
    with pytest.raises(PermissionError):
        ledger.transition("cell_a", "promoted")

    # Self-certification (maker_id == checker_id) must fail closed
    with pytest.raises(PermissionError):
        ledger.transition(
            "cell_a",
            "verified",
            maker_id="maker_alpha",
            checker_id="maker_alpha",
        )

    # Multi-checker quorum holds state at evaluated until quorum is reached
    partial_state = ledger.transition(
        "cell_a",
        "verified",
        maker_id="maker_alpha",
        checker_id="checker_one",
        quorum_needed=quorum_two,
    )
    assert partial_state == "evaluated"

    verified_state = ledger.transition(
        "cell_a",
        "verified",
        maker_id="maker_alpha",
        checker_id="checker_two",
        quorum_needed=quorum_two,
    )
    assert verified_state == "verified"
    assert ledger.transition("cell_a", "promoted") == "promoted"
    assert ledger.records["cell_a"]["history"] == list(LIFECYCLE_STATES)


def test_fenced_task_store_zombie_epoch_rejection_and_resume_packet(tmp_path: Path) -> None:
    """Verify CAS lease_epoch fencing blocks zombie overwrites and caps resume packets."""
    grid = NodeGrid.default()
    tv = grid.test_vectors
    t0 = float(tv["base_timestamp"])
    t_step = float(tv["step_timestamp"])
    t_exp = t0 + float(tv["expired_advance_seconds"])

    store = FencedTaskStore(db_path=tmp_path / "fenced_store.sqlite3", grid=grid)
    enq = store.enqueue_task(
        task_id="stage_task_01",
        namespace="composer",
        verb="compile_unit",
        payload={"module": "core_router"},
        contract_digest="v1",
        now_ts=t0,
    )
    assert enq["ok"] is True

    claim_1 = store.claim_task(
        worker_id="worker_stalled",
        task_id="stage_task_01",
        now_ts=t0 + t_step,
    )
    assert claim_1["ok"] is True
    assert claim_1["lease_epoch"] == 1
    byte_cap = int(grid.resolve_cell("durable_multi_worker")["resume_packet_byte_cap"])
    assert claim_1["resume_packet_bytes"] <= byte_cap

    # Lease expires; second worker claims epoch 2
    claim_2 = store.claim_task(
        worker_id="worker_active",
        task_id="stage_task_01",
        now_ts=t_exp,
    )
    assert claim_2["ok"] is True
    assert claim_2["lease_epoch"] > claim_1["lease_epoch"]

    # Stalled worker wakes up and tries to checkpoint with stale epoch 1 -> rejected
    fence_1 = str(claim_1["fence_token"])
    stale = store.checkpoint_task(
        task_id="stage_task_01",
        worker_id="worker_stalled",
        lease_epoch=int(claim_1["lease_epoch"]),
        fence_token=fence_1,
        checkpoint={"corrupted": True},
        now_ts=t_exp + t_step,
    )
    assert stale["ok"] is False
    assert stale["reason"] == "stale_epoch_rejected"

    # Large checkpoint triggers resume_packet_byte_cap auto-expansion while epoch 2 succeeds
    repeat_n = int(tv["oversized_checkpoint_repeat"])
    large_blob = {"notes": ["segment_state_payload_entry"] * repeat_n}
    fence_2 = str(claim_2["fence_token"])
    valid_ckpt = store.checkpoint_task(
        task_id="stage_task_01",
        worker_id="worker_active",
        lease_epoch=int(claim_2["lease_epoch"]),
        fence_token=fence_2,
        checkpoint=large_blob,
        completed=True,
        now_ts=t_exp + t_step,
    )
    assert valid_ckpt["ok"] is True
    assert valid_ckpt["grid_boundary_expanded"] is True


def test_iterative_artifact_composer_dag_validation_and_saturation_expansion(tmp_path: Path) -> None:
    """Verify cycle detection, unknown dependency rejection, and max_composer_stages expansion."""
    grid = NodeGrid.default()
    tv = grid.test_vectors
    t0 = float(tv["base_timestamp"])
    sat_count = int(tv["saturated_stage_count"])

    store = FencedTaskStore(db_path=tmp_path / "dag_check.sqlite3", grid=grid)
    composer = IterativeArtifactComposer(store, grid=grid)

    cyclic = [
        StageSpec(stage_id="s_a", verb="step", payload={}, depends_on=("s_b",)),
        StageSpec(stage_id="s_b", verb="step", payload={}, depends_on=("s_a",)),
    ]
    with pytest.raises(ValueError):
        composer.topological_sort(cyclic)

    missing_dep = [
        StageSpec(stage_id="s_a", verb="step", payload={}, depends_on=("nonexistent",)),
    ]
    with pytest.raises(ValueError):
        composer.topological_sort(missing_dep)

    # Saturate initial max_composer_stages bound -> automatic boundary expansion
    chain_stages: List[StageSpec] = []
    for idx in range(sat_count):
        sid = f"stage_{idx}"
        deps = (f"stage_{idx - 1}",) if idx >= 1 else ()
        chain_stages.append(
            StageSpec(stage_id=sid, verb="transform", payload={"rank": idx}, depends_on=deps)
        )

    old_gen = grid.generation
    plan = composer.decompose_build(build_id="saturated_build", stages=chain_stages, now_ts=t0)
    assert plan["grid_expanded"] is True
    assert grid.generation == old_gen + 1
    assert len(plan["stage_order"]) == sat_count


def test_iterative_artifact_composer_deterministic_manifest_and_bitemporal_reuse(
    tmp_path: Path,
) -> None:
    """Verify multi-stage composition determinism, self-certification rejection, and cache reuse."""
    grid_a = NodeGrid.default()
    t0 = float(grid_a.test_vectors["base_timestamp"])
    t_step = float(grid_a.test_vectors["step_timestamp"])

    stages = [
        StageSpec(
            stage_id="package",
            verb="bundle_manifest",
            payload={"target": "release"},
            depends_on=("compile_a", "compile_b"),
        ),
        StageSpec(
            stage_id="schema_init",
            verb="emit_schema",
            payload={"version": "v1"},
            depends_on=(),
        ),
        StageSpec(
            stage_id="compile_b",
            verb="compile_module",
            payload={"unit": "worker_pool"},
            depends_on=("schema_init",),
        ),
        StageSpec(
            stage_id="compile_a",
            verb="compile_module",
            payload={"unit": "lease_router"},
            depends_on=("schema_init",),
        ),
    ]

    maker_invocations: List[str] = []

    def maker_fn(ctx: Dict[str, Any]) -> Mapping[str, object]:
        maker_invocations.append(str(ctx["stage_id"]))
        return {
            "stage": ctx["stage_id"],
            "verb": ctx["verb"],
            "upstream_count": len(ctx["upstream_digests"]),
            "upstream_digests": dict(ctx["upstream_digests"]),
        }

    def checker_fn(ctx: Dict[str, Any], candidate: Mapping[str, object]) -> bool:
        return candidate.get("stage") == ctx["stage_id"]

    store_a = FencedTaskStore(db_path=tmp_path / "compose_a.sqlite3", grid=grid_a)
    composer_a = IterativeArtifactComposer(store_a, grid=grid_a)

    # Self-certification (maker_id == checker_id) is blocked
    with pytest.raises(PermissionError):
        composer_a.execute_and_compose(
            build_id="build_self_cert",
            stages=stages,
            maker_id="agent_same",
            checker_id="agent_same",
            maker_fn=maker_fn,
            checker_fn=checker_fn,
            now_ts=t0,
        )

    manifest_1 = composer_a.execute_and_compose(
        build_id="build_run_01",
        stages=stages,
        maker_id="maker_worker_01",
        checker_id="checker_worker_02",
        maker_fn=maker_fn,
        checker_fn=checker_fn,
        now_ts=t0,
    )
    assert manifest_1["stage_order"] == ["schema_init", "compile_a", "compile_b", "package"]
    assert manifest_1["executed_stage_count"] == len(stages)
    assert manifest_1["reused_stage_count"] == 0
    assert manifest_1["cell_lifecycle_state"] == "promoted"

    # Re-running an identical stage spec within valid_until reuses 100% of verified stages (0 maker calls)
    invocations_before = len(maker_invocations)
    manifest_cached = composer_a.execute_and_compose(
        build_id="build_run_02_cached",
        stages=stages,
        maker_id="maker_worker_01",
        checker_id="checker_worker_02",
        maker_fn=maker_fn,
        checker_fn=checker_fn,
        now_ts=t0 + t_step,
    )
    assert len(maker_invocations) == invocations_before
    assert manifest_cached["executed_stage_count"] == 0
    assert manifest_cached["reused_stage_count"] == len(stages)
    assert manifest_cached["manifest_digest"] == manifest_1["manifest_digest"]

    # Independent fresh store with reversed input stage list produces bit-identical manifest_digest
    grid_b = NodeGrid.default()
    store_b = FencedTaskStore(db_path=tmp_path / "compose_b.sqlite3", grid=grid_b)
    composer_b = IterativeArtifactComposer(store_b, grid=grid_b)
    manifest_fresh = composer_b.execute_and_compose(
        build_id="build_independent",
        stages=list(reversed(stages)),
        maker_id="maker_worker_09",
        checker_id="checker_worker_10",
        maker_fn=maker_fn,
        checker_fn=checker_fn,
        now_ts=t0,
    )
    assert manifest_fresh["manifest_digest"] == manifest_1["manifest_digest"]


def test_mcp_stdio_bridge_backward_compatibility(tmp_path: Path) -> None:
    """Ensure the existing JSON-RPC 2.0 Stdio MCP bridge remains fully operational."""
    db_path = tmp_path / "bridge_compat.sqlite3"
    req_line = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}) + "\n"
    proc = subprocess.run(
        [sys.executable, str(BRIDGE_SCRIPT), "--db", str(db_path), "--mcp-stdio"],
        input=req_line,
        capture_output=True,
        text=True,
        check=True,
    )
    reply = json.loads(proc.stdout.strip())
    assert len(reply["result"]["tools"]) >= 1
