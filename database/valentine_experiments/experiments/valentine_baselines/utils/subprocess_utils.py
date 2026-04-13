from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any


def run_subprocess(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    timeout_sec: int = 3600,
) -> dict[str, Any]:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd is not None else None,
        capture_output=True,
        text=True,
        timeout=timeout_sec,
        check=False,
    )
    return {
        "command": cmd,
        "cwd": str(cwd) if cwd is not None else None,
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }

