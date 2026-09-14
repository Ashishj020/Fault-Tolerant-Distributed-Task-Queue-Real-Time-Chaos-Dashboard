from __future__ import annotations

import hashlib
import math
import random
import time
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageFilter

from models.job import Job, JobType


class TaskError(Exception):
    def __init__(self, message: str, retryable: bool = True) -> None:
        super().__init__(message)
        self.retryable = retryable


def _duration_for(job: Job) -> float:
    payload = job.payload or {}
    if "duration" in payload:
        return float(payload["duration"])
    ranges = {
        JobType.IMAGE_PROCESSING: (5.0, 12.0),
        JobType.REPORT_GENERATION: (3.0, 9.0),
        JobType.DATA_TRANSFORMATION: (2.0, 7.0),
    }
    low, high = ranges.get(job.type, (3.0, 8.0))
    seed = int(hashlib.sha256(job.idempotency_key.encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)
    complexity = str(payload.get("complexity", "medium"))
    if complexity == "low":
        high = (low + high) / 2
    elif complexity == "high":
        low = (low + high) / 2
    return rng.uniform(low, high)


def _should_fail(job: Job) -> str | None:
    payload = job.payload or {}
    if payload.get("force_fail"):
        return str(payload.get("fail_reason") or "forced failure")
    fail_until = payload.get("fail_until_attempt")
    if fail_until is not None and job.attempt < int(fail_until):
        return f"synthetic failure until attempt {fail_until}"
    return None


def _busy_compute(seed: int, seconds: float) -> tuple[float, str]:
    """Deterministic numeric work that cannot be optimized into a no-op sleep."""
    deadline = time.perf_counter() + seconds
    rng = np.random.default_rng(seed)
    matrix = rng.standard_normal((96, 96))
    checksum = 0.0
    passes = 0
    kernel = np.array([[0, -1, 0], [-1, 4, -1], [0, -1, 0]], dtype=float)
    while time.perf_counter() < deadline:
        padded = np.pad(matrix, 1, mode="edge")
        acc = np.zeros_like(matrix)
        for i in range(3):
            for j in range(3):
                acc += kernel[i, j] * padded[i : i + matrix.shape[0], j : j + matrix.shape[1]]
        matrix = np.tanh(acc) * 0.15 + matrix * 0.85
        checksum = float(np.linalg.norm(matrix))
        passes += 1
    digest = hashlib.sha256(f"{seed}:{checksum:.6f}:{passes}".encode()).hexdigest()
    return checksum, digest


def process_image(job: Job, results_dir: Path) -> dict[str, Any]:
    source = str((job.payload or {}).get("input") or "sample.jpg")
    seed = int(hashlib.sha256(f"{source}:{job.idempotency_key}".encode()).hexdigest()[:8], 16)
    rng = np.random.default_rng(seed)
    width, height = 96, 96
    noise = (rng.random((height, width, 3)) * 255).astype(np.uint8)
    # Paint a deterministic gradient so the "image" is meaningful, not random junk.
    ys = np.linspace(0, 255, height, dtype=np.uint8)
    xs = np.linspace(0, 255, width, dtype=np.uint8)
    noise[:, :, 0] = (noise[:, :, 0] // 4 + ys[:, None] * 3 // 4).astype(np.uint8)
    noise[:, :, 1] = (noise[:, :, 1] // 4 + xs[None, :] * 3 // 4).astype(np.uint8)
    image = Image.fromarray(noise, mode="RGB")
    duration = _duration_for(job)
    started = time.perf_counter()
    passes = 0
    while time.perf_counter() - started < duration:
        image = image.filter(ImageFilter.FIND_EDGES).filter(ImageFilter.SMOOTH_MORE)
        passes += 1
        if passes > 40:
            break
    remaining = duration - (time.perf_counter() - started)
    checksum, digest = _busy_compute(seed, max(0.2, remaining))
    results_dir.mkdir(parents=True, exist_ok=True)
    output_path = results_dir / f"{job.idempotency_key}.png"
    image.save(output_path)
    histogram = [int(v) for v in np.array(image.convert("L").histogram()[::16])]
    return {
        "type": job.type.value,
        "input": source,
        "output_path": str(output_path),
        "passes": passes,
        "checksum": checksum,
        "digest": digest,
        "histogram": histogram,
        "duration_s": round(time.perf_counter() - started, 3),
    }


def generate_report(job: Job, results_dir: Path) -> dict[str, Any]:
    title = str((job.payload or {}).get("title") or (job.payload or {}).get("input") or "quarterly")
    seed = int(hashlib.sha256(f"{title}:{job.idempotency_key}".encode()).hexdigest()[:8], 16)
    rng = np.random.default_rng(seed)
    rows = 1800
    series = rng.normal(loc=42.0, scale=7.5, size=rows)
    duration = _duration_for(job)
    checksum, digest = _busy_compute(seed, duration)
    stats = {
        "count": int(rows),
        "mean": float(series.mean()),
        "stdev": float(series.std()),
        "p50": float(np.percentile(series, 50)),
        "p95": float(np.percentile(series, 95)),
        "min": float(series.min()),
        "max": float(series.max()),
    }
    lines = [
        f"# Report: {title}",
        f"idempotency_key: {job.idempotency_key}",
        f"mean: {stats['mean']:.4f}",
        f"p95: {stats['p95']:.4f}",
        f"digest: {digest}",
        "",
        "## Histogram buckets",
    ]
    hist, _ = np.histogram(series, bins=12)
    for idx, bucket in enumerate(hist.tolist()):
        lines.append(f"- bucket_{idx}: {bucket}")
    results_dir.mkdir(parents=True, exist_ok=True)
    output_path = results_dir / f"{job.idempotency_key}.md"
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return {
        "type": job.type.value,
        "title": title,
        "output_path": str(output_path),
        "stats": stats,
        "digest": digest,
        "checksum": checksum,
        "duration_s": duration,
    }


def transform_data(job: Job, results_dir: Path) -> dict[str, Any]:
    dataset = str((job.payload or {}).get("dataset") or (job.payload or {}).get("input") or "events")
    seed = int(hashlib.sha256(f"{dataset}:{job.idempotency_key}".encode()).hexdigest()[:8], 16)
    rng = np.random.default_rng(seed)
    n = 12_000
    keys = rng.integers(0, 64, size=n)
    values = rng.random(n)
    duration = _duration_for(job)
    checksum, digest = _busy_compute(seed, duration * 0.55)
    started = time.perf_counter()
    grouped: dict[int, float] = {}
    for key, value in zip(keys.tolist(), values.tolist()):
        grouped[key] = grouped.get(key, 0.0) + math.sin(value) * math.cos(key)
    ranked = sorted(grouped.items(), key=lambda item: item[1], reverse=True)
    remaining = duration - (time.perf_counter() - started)
    extra_checksum, extra_digest = _busy_compute(seed + 7, max(0.2, remaining))
    results_dir.mkdir(parents=True, exist_ok=True)
    output_path = results_dir / f"{job.idempotency_key}.json"
    output_path.write_text(
        str(
            {
                "dataset": dataset,
                "groups": len(ranked),
                "top": ranked[:8],
                "digest": digest,
                "extra_digest": extra_digest,
            }
        ),
        encoding="utf-8",
    )
    return {
        "type": job.type.value,
        "dataset": dataset,
        "groups": len(ranked),
        "top_key": ranked[0][0] if ranked else None,
        "top_value": ranked[0][1] if ranked else None,
        "digest": hashlib.sha256(f"{digest}:{extra_digest}:{checksum}".encode()).hexdigest(),
        "checksum": checksum + extra_checksum,
        "output_path": str(output_path),
        "duration_s": duration,
    }


def run_task(job: Job, results_dir: str | Path) -> dict[str, Any]:
    reason = _should_fail(job)
    if reason:
        raise TaskError(reason, retryable=True)
    path = Path(results_dir)
    if job.type == JobType.IMAGE_PROCESSING:
        return process_image(job, path)
    if job.type == JobType.REPORT_GENERATION:
        return generate_report(job, path)
    if job.type == JobType.DATA_TRANSFORMATION:
        return transform_data(job, path)
    raise TaskError(f"unknown job type {job.type}", retryable=False)
