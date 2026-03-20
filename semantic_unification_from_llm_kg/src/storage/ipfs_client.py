# src/storage/ipfs_client.py
import json
from typing import Any
import requests

from src.configs.config import IPFS_API_URL, IPFS_HTTP_TIMEOUT_SEC


class IPFSClient:
    """
    与 IPFS 通信的客户端：
    - 不关心 LLM / KG / 数据库，只处理：Python 对象 <-> JSON <-> CID
    """

    def __init__(self, api_url: str = IPFS_API_URL, timeout: int = IPFS_HTTP_TIMEOUT_SEC):
        self.api_url = api_url.rstrip("/")
        self.timeout = timeout

    # ---------- JSON 对象上传/下载 ----------

    def add_json(self, obj: Any) -> str:
        """
        将 Python 对象作为 JSON 上传到 IPFS，返回 CID (Hash)
        """
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        files = {"file": ("data.json", data)}

        resp = requests.post(
            f"{self.api_url}/add",
            files=files,
            timeout=self.timeout
        )
        resp.raise_for_status()

        result = resp.json()
        cid = result["Hash"]
        print(f"已上传IPFS，CID = {cid}")
        return cid

    def cat_json(self, cid: str) -> Any:
        """
        根据 CID 从 IPFS 拉取 JSON，并反序列化为 Python 对象
        """
        params = {"arg": cid}
        resp = requests.post(
            f"{self.api_url}/cat",
            params=params,
            timeout=self.timeout
        )
        resp.raise_for_status()
        text = resp.text
        obj = json.loads(text)
        print(f"IPFS内容已获取，CID = {cid}")
        return obj

    # ---------- 文件上传（可选） ----------

    def add_file(self, filepath: str) -> str:
        """
        上传一个本地文件到 IPFS，返回 CID
        """
        with open(filepath, "rb") as f:
            files = {"file": (filepath, f)}
            resp = requests.post(
                f"{self.api_url}/add",
                files=files,
                timeout=self.timeout
            )
            resp.raise_for_status()
            result = resp.json()
            cid = result["Hash"]
            print(f"[IPFS] 已上传文件 {filepath}，CID = {cid}")
            return cid

    def cat_raw(self, cid: str) -> bytes:
        """
        拉取原始字节（如果以后你要存二进制，比如模型文件）
        """
        params = {"arg": cid}
        resp = requests.post(
            f"{self.api_url}/cat",
            params=params,
            timeout=self.timeout
        )
        resp.raise_for_status()
        print(f"[IPFS] 已获取原始数据，CID = {cid}")
        return resp.content
