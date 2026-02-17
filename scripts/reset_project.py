#!/usr/bin/env python3
import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = REPO_ROOT / "kafka_spark_notebook.ipynb"


def run_cmd(cmd: list[str], *, check: bool = True) -> int:
    print(f"[reset] $ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=REPO_ROOT)
    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(cmd)}")
    return result.returncode


def best_effort_kill(pattern: str) -> None:
    rc = run_cmd(["pkill", "-f", pattern], check=False)
    if rc == 0:
        print(f"[reset] stopped processes matching: {pattern}")
    elif rc == 1:
        print(f"[reset] no running processes matching: {pattern}")
    else:
        print(f"[reset] warning: pkill returned code {rc} for pattern: {pattern}")


def cleanup_jupyter_runtime() -> None:
    runtime_candidates = [
        Path.home() / "Library" / "Jupyter" / "runtime",
        Path.home() / ".local" / "share" / "jupyter" / "runtime",
    ]
    removed_any = False
    for runtime_dir in runtime_candidates:
        if not runtime_dir.is_dir():
            continue
        for pattern in ("kernel-*.json", "nbserver-*.json"):
            for runtime_file in runtime_dir.glob(pattern):
                try:
                    runtime_file.unlink()
                    removed_any = True
                except OSError:
                    print(f"[reset] warning: could not remove {runtime_file}")
    if removed_any:
        print("[reset] removed Jupyter runtime connection files")
    else:
        print("[reset] no Jupyter runtime files to remove")


def clear_notebook_outputs(notebook_path: Path) -> None:
    if not notebook_path.exists():
        print(f"[reset] notebook not found, skipping: {notebook_path}")
        return

    with notebook_path.open("r", encoding="utf-8") as f:
        notebook = json.load(f)

    changed = False
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        if cell.get("outputs"):
            cell["outputs"] = []
            changed = True
        if cell.get("execution_count") is not None:
            cell["execution_count"] = None
            changed = True

    if changed:
        with notebook_path.open("w", encoding="utf-8") as f:
            json.dump(notebook, f, ensure_ascii=False, indent=1)
            f.write("\n")
        print(f"[reset] cleared notebook outputs: {notebook_path.name}")
    else:
        print(f"[reset] notebook already clean: {notebook_path.name}")


def reset_directories() -> None:
    data_dir = REPO_ROOT / "data"
    output_dir = REPO_ROOT / "output"

    if data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    print(f"[reset] recreated: {data_dir}")

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"[reset] recreated: {output_dir}")


def wait_for_kafka(max_attempts: int = 20, delay_seconds: float = 1.5) -> None:
    check_cmd = [
        "docker",
        "exec",
        "kafka-1",
        "kafka-topics",
        "--list",
        "--bootstrap-server",
        "kafka-1:9092,kafka-2:9092",
    ]
    for attempt in range(1, max_attempts + 1):
        rc = run_cmd(check_cmd, check=False)
        if rc == 0:
            print("[reset] kafka is ready")
            return
        print(f"[reset] waiting for kafka ({attempt}/{max_attempts})...")
        time.sleep(delay_seconds)
    raise RuntimeError("Kafka did not become ready in time.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Hard reset for demo: Kafka, topic, local data, and notebook outputs."
    )
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--partitions", type=int, default=6)
    parser.add_argument("--replication-factor", type=int, default=2)
    parser.add_argument(
        "--skip-kill-processes",
        action="store_true",
        help="Do not kill Jupyter/Spark local processes before reset.",
    )
    parser.add_argument(
        "--skip-clear-notebook",
        action="store_true",
        help="Do not clear notebook outputs/execution counters.",
    )
    parser.add_argument(
        "--skip-kafka-restart",
        action="store_true",
        help="Do not restart dockerized Kafka (still clears local files).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.skip_kill_processes:
        print("[reset] stopping local Jupyter/Spark processes (best effort)")
        best_effort_kill("jupyter-notebook")
        best_effort_kill("jupyter-lab")
        best_effort_kill("ipykernel_launcher")
        best_effort_kill("spark-submit")
        best_effort_kill("org.apache.spark.deploy.SparkSubmit")
        cleanup_jupyter_runtime()

    if not args.skip_kafka_restart:
        run_cmd(["docker", "compose", "down", "-v"])
        run_cmd(["docker", "compose", "up", "-d"])
        wait_for_kafka()
        run_cmd(
            [
                str(REPO_ROOT / "scripts" / "create_topic.sh"),
                args.topic,
                str(args.partitions),
                str(args.replication_factor),
            ]
        )
    else:
        print("[reset] skipped kafka restart")

    reset_directories()

    if not args.skip_clear_notebook:
        clear_notebook_outputs(NOTEBOOK_PATH)
    else:
        print("[reset] skipped notebook output cleanup")

    print("[reset] done")
    print("[reset] next: start producer/tracker and run notebook from the first cell")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"[reset][error] {exc}", file=sys.stderr)
        raise SystemExit(1)
