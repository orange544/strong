# src/storage/ipfs_client.py
import json
from typing import Any

import requests

from src.configs.config import IPFS_API_URL, IPFS_HTTP_TIMEOUT_SEC

__all__ = ["IPFSClient", "requests"]


def _extract_cid(result: object) -> str:
    if not isinstance(result, dict):
        raise RuntimeError("IPFS add response must be a JSON object")
    raw_cid = result.get("Hash")
    if not isinstance(raw_cid, str) or not raw_cid.strip():
        raise RuntimeError("IPFS add response missing non-empty 'Hash'")
    return raw_cid.strip()


class IPFSClient:
    """
    Minimal IPFS HTTP client used by the pipeline.
    It handles only object/file <-> CID operations and stays decoupled
    from LLM/KG logic.
    """

    def __init__(self, api_url: str = IPFS_API_URL, timeout: int = IPFS_HTTP_TIMEOUT_SEC):
        self.api_url = api_url.rstrip("/")
        self.timeout = timeout

    # JSON object upload/download

    def add_json(self, obj: Any) -> str:
        """
        Upload a Python object as JSON and return the resulting CID.
        """
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        files = {"file": ("data.json", data)}

        resp = requests.post(
            f"{self.api_url}/add",
            files=files,
            timeout=self.timeout,
        )
        resp.raise_for_status()

        result = resp.json()
        cid = _extract_cid(result)
        print(f"[IPFS] Uploaded JSON, CID={cid}")
        return cid

    def cat_json(self, cid: str) -> Any:
        """
        Fetch JSON content by CID and deserialize to a Python object.
        """
        params = {"arg": cid}
        resp = requests.post(
            f"{self.api_url}/cat",
            params=params,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        text = resp.text
        obj = json.loads(text)
        print(f"[IPFS] Fetched JSON, CID={cid}")
        return obj

    # File upload/download

    def add_file(self, filepath: str) -> str:
        """
        Upload a local file and return the resulting CID.
        """
        with open(filepath, "rb") as f:
            files = {"file": (filepath, f)}
            resp = requests.post(
                f"{self.api_url}/add",
                files=files,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            result = resp.json()
            cid = _extract_cid(result)
            print(f"[IPFS] Uploaded file: {filepath}, CID={cid}")
            return cid

    def cat_raw(self, cid: str) -> bytes:
        """
        Fetch raw bytes by CID.
        """
        params = {"arg": cid}
        resp = requests.post(
            f"{self.api_url}/cat",
            params=params,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        print(f"[IPFS] Fetched raw content, CID={cid}")
        return resp.content
