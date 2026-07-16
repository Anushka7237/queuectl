from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def run(cmd: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    return result


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        env = os.environ.copy()
        env["QUEUECTL_DATA_DIR"] = str(data_dir)
        queuectl = [sys.executable, "-m", "queuectl.cli"]

        print("1. Configure queue")
        run(queuectl + ["config", "set", "max-retries", "2"], env)
        run(queuectl + ["config", "set", "backoff-base", "1"], env)

        print("2. Enqueue success and failure jobs")
        run(
            queuectl
            + [
                "enqueue",
                json.dumps({"id": "ok-job", "command": "echo validation-ok"}),
            ],
            env,
        )
        run(
            queuectl
            + [
                "enqueue",
                json.dumps({"id": "fail-job", "command": "exit 1", "max_retries": 2}),
            ],
            env,
        )

        print("3. Start workers")
        worker = subprocess.Popen(
            queuectl + ["worker", "start", "--count", "2"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        print("4. Wait for jobs to settle")
        deadline = time.time() + 30
        while time.time() < deadline:
            status = run(queuectl + ["status"], env)
            if "completed" in status.stdout and "dead" in status.stdout:
                completed = run(queuectl + ["list", "--state", "completed"], env)
                dead = run(queuectl + ["dlq", "list"], env)
                if "ok-job" in completed.stdout and "fail-job" in dead.stdout:
                    break
            time.sleep(0.5)
        else:
            worker.terminate()
            raise RuntimeError("Validation timed out waiting for job completion")

        print("5. Stop workers")
        run(queuectl + ["worker", "stop"], env)
        worker.wait(timeout=10)

        print("6. Retry DLQ job")
        run(queuectl + ["dlq", "retry", "fail-job"], env)
        pending = run(queuectl + ["list", "--state", "pending"], env)
        if "fail-job" not in pending.stdout:
            raise RuntimeError("DLQ retry did not requeue job")

        print("All validation checks passed.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
