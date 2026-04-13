from __future__ import annotations

import os
import platform
import shutil
import subprocess
from pathlib import Path
from typing import Any


def command_exists(cmd: str) -> bool:
    return shutil.which(cmd) is not None


def detect_java() -> dict[str, Any]:
    if not command_exists("java"):
        return {"ok": False, "message": "java not found in PATH"}
    try:
        proc = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            check=False,
            timeout=20,
        )
    except Exception as exc:  # pragma: no cover - environment dependent
        return {"ok": False, "message": f"java check failed: {exc!r}"}

    text = (proc.stderr or "") + "\n" + (proc.stdout or "")
    first_line = text.strip().splitlines()[0] if text.strip() else "unknown"
    return {"ok": True, "message": first_line}


def ensure_nltk_data(nltk_data_dir: Path) -> None:
    os.environ["NLTK_DATA"] = str(nltk_data_dir)


def apply_windows_wmi_import_workaround() -> None:
    """
    Work around occasional hangs in platform WMI queries on some Windows setups.

    This is needed because importing Valentine pulls in NLTK/SciPy/NumPy, and NumPy may
    call platform.machine(), which can block in platform._wmi_query().
    """
    if os.name != "nt":
        return

    if hasattr(platform, "_wmi_query"):
        platform._wmi_query = lambda *args, **kwargs: [""]  # type: ignore[assignment]
    if hasattr(platform, "_get_machine_win32"):
        platform._get_machine_win32 = lambda: ""  # type: ignore[assignment]
    platform.win32_ver = (  # type: ignore[assignment]
        lambda release="", version="", csd="", ptype="": ("", "", "", "")
    )
