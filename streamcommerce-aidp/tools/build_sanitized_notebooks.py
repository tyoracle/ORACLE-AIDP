#!/usr/bin/env python3
"""Build output-free, environment-neutral notebooks and Python exports."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

NOTEBOOKS = (
    (
        Path("/private/tmp/01_kafka_to_bronze_workflow_v2.executed.ipynb"),
        Path("notebooks/bronze/01_kafka_to_bronze_workflow_v2.ipynb"),
    ),
    (
        Path(
            "/private/tmp/"
            "02_bronze_to_silver_timed_streaming_workflow_dq_v3_fixed_bounds.executed.ipynb"
        ),
        Path(
            "notebooks/silver/"
            "02_bronze_to_silver_timed_streaming_workflow_dq_v3_fixed_bounds.ipynb"
        ),
    ),
    (
        Path("/private/tmp/aidp_task_run_output.ipynb"),
        Path(
            "notebooks/gold/"
            "04d_silver_to_dual_gold_workflow_v3_3_timer_retry_safe.ipynb"
        ),
    ),
    (
        Path(
            "/private/tmp/"
            "05_inventory_stockout_alert_workflow_v3_0_agent_function.executed.ipynb"
        ),
        Path(
            "notebooks/alert/"
            "05_inventory_stockout_alert_workflow_v3_0_agent_function.ipynb"
        ),
    ),
)

KAFKA_BOOTSTRAP_PATTERN = re.compile(
    r"bootstrap-[A-Za-z0-9-]+\.kafka\.[A-Za-z0-9-]+\.oci\.oraclecloud\.com:\d+",
    re.IGNORECASE,
)

SENSITIVE_PATTERNS = (
    re.compile(r"ocid1\.[a-z0-9.-]{30,}", re.IGNORECASE),
    re.compile(r"https://[^\s\"']*(?:agents|functions)[^\s\"']+", re.IGNORECASE),
    re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE),
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
)


def source_text(cell: dict) -> str:
    source = cell.get("source", "")
    return "".join(source) if isinstance(source, list) else str(source)


def sanitized_text(text: str) -> str:
    return KAFKA_BOOTSTRAP_PATTERN.sub("kafka.example.invalid:9092", text)


def sanitize_notebook(source_path: Path) -> dict:
    notebook = json.loads(source_path.read_text(encoding="utf-8"))
    clean_cells = []

    release_note = {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "<!-- sanitized-release -->\n",
            "This notebook is a source-only Git release artifact. Execution outputs, "
            "environment endpoints, and credentials are intentionally excluded.\n",
        ],
    }
    clean_cells.append(release_note)

    for cell in notebook.get("cells", []):
        clean_cell = {
            "cell_type": cell.get("cell_type", "code"),
            "metadata": {},
            "source": sanitized_text(source_text(cell)).splitlines(keepends=True),
        }
        tags = cell.get("metadata", {}).get("tags")
        if tags:
            clean_cell["metadata"]["tags"] = tags
        if clean_cell["cell_type"] == "code":
            clean_cell["execution_count"] = None
            clean_cell["outputs"] = []
        clean_cells.append(clean_cell)

    metadata = notebook.get("metadata", {})
    clean_metadata = {}
    for key in ("kernelspec", "language_info"):
        if key in metadata:
            clean_metadata[key] = metadata[key]

    return {
        "cells": clean_cells,
        "metadata": clean_metadata,
        "nbformat": notebook.get("nbformat", 4),
        "nbformat_minor": notebook.get("nbformat_minor", 5),
    }


def python_export(notebook: dict) -> str:
    chunks = [
        '"""Source export generated from the sanitized AIDP notebook."""\n'
    ]
    code_index = 0
    for cell in notebook["cells"]:
        if cell["cell_type"] != "code":
            continue
        code_index += 1
        chunks.append(f"\n# %% [code cell {code_index}]\n")
        text = source_text(cell)
        chunks.append(text)
        if text and not text.endswith("\n"):
            chunks.append("\n")
    return "".join(chunks)


def assert_no_sensitive_content(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    findings = []
    for pattern in SENSITIVE_PATTERNS:
        findings.extend(match.group(0) for match in pattern.finditer(text))
    if findings:
        raise RuntimeError(f"Sensitive content detected in {path}: {findings[:3]}")


def main() -> None:
    manifest = {"generated_files": []}
    for source_path, relative_target in NOTEBOOKS:
        if not source_path.exists():
            raise FileNotFoundError(source_path)

        notebook = sanitize_notebook(source_path)
        notebook_target = PROJECT_ROOT / relative_target
        notebook_target.parent.mkdir(parents=True, exist_ok=True)
        notebook_target.write_text(
            json.dumps(notebook, ensure_ascii=False, indent=1) + "\n",
            encoding="utf-8",
        )

        python_target = notebook_target.with_suffix(".py")
        python_target.write_text(python_export(notebook), encoding="utf-8")

        for target in (notebook_target, python_target):
            assert_no_sensitive_content(target)
            manifest["generated_files"].append(
                {
                    "path": str(target.relative_to(PROJECT_ROOT)),
                    "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
                }
            )

    manifest_path = PROJECT_ROOT / "notebooks" / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Generated {len(manifest['generated_files'])} sanitized artifacts.")


if __name__ == "__main__":
    main()
