#这是新增抽样结果到IPFS后的抽样层，对新增数据库进行抽样，并上传样本数据到IPFS
from datetime import datetime

from src.db.database_agent import DatabaseAgent, get_all_fields
from src.storage.ipfs_client import IPFSClient
from src.utils.io import save_json


def run_sampling(
    db_agents: dict[str, DatabaseAgent],
    ipfs: IPFSClient,
    timestamp: str | None = None,
) -> str:
    """
    Step1: 对新增数据库进行抽样，将样本数据上传到 IPFS
    返回：samples_cid
    """
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    all_samples: list[dict[str, object]] = []
    for db_name, agent in db_agents.items():
        print(f"正在抽样数据库: {db_name}")
        samples = get_all_fields(agent)  # 获取字段样本
        for s in samples:
            # 加上 db_name 信息
            if "db_name" not in s:
                s["db_name"] = db_name
        all_samples.extend(samples)

    samples_file = f"samples_{timestamp}.json"
    save_json(all_samples, samples_file)

    samples_cid = ipfs.add_json(all_samples)
    if not isinstance(samples_cid, str):
        raise RuntimeError("ipfs.add_json must return a CID string")
    print(f"抽样完成，共 {len(all_samples)} 条样本，CID = {samples_cid}")

    return samples_cid
