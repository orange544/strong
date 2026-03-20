from typing import List, Dict
from src.storage.ipfs_client import IPFSClient
from src.utils.io import save_json
from src.llm.description_agent import FieldDescriptionAgent
from src.llm.semantic import FieldSemanticAgent


def run_llm_pipeline(
    ipfs: IPFSClient,
    samples_cid: str,
    timestamp: str | None = None,
    llm_config: Dict[str, str] | None = None,
) -> List[Dict]:
    """
    从 IPFS 拉取样本数据，生成字段描述并进行语义比对
    """
    
    if llm_config is None:
        llm_config = {}

    print(f"从 IPFS 拉取样本，CID = {samples_cid}")
    samples = ipfs.cat_json(samples_cid)

    # ---------- Step 1: 字段描述生成 ----------
    fd_agent = FieldDescriptionAgent(
        api_key=llm_config.get("api_key", ""),
        base_url=llm_config.get("base_url", ""),
        model_name=llm_config.get("model_name", ""),
    )

    print("正在生成字段描述...")
    field_descriptions = [fd_agent.generate_description(f) for f in samples]

    fd_file = f"field_descriptions_{timestamp}.json"
    save_json(field_descriptions, fd_file)

    return field_descriptions


def update_unified_fields_with_new_descriptions(
    previous_unified_fields: List[Dict], new_field_descriptions: List[Dict]
) -> List[Dict]:
    """
    对比现有的 unified_fields 和新生成的字段描述。
    如果新字段描述与某个标准字段描述相似，就将其加入该标准字段下；
    否则创建新的标准字段。
    """
    updated_unified_fields = []

    for new_field in new_field_descriptions:
        field_description = new_field.get("description")
        field_name = new_field.get("field")
        matched = False

        # 对比现有 unified_fields
        for unified_field in previous_unified_fields:
            if unified_field["description"] == field_description:
                # 如果描述相同，将新字段加入该标准字段
                unified_field["fields"].append(field_name)
                matched = True
                break
        
        if not matched:
            # 如果没有匹配项，创建新的标准字段
            new_unified_field = {
                "canonical_name": field_name,  # 新字段的标准名称
                "fields": [field_name],         # 当前字段名作为字段列表
                "description": field_description
            }
            updated_unified_fields.append(new_unified_field)

    # 将现有的标准字段和新标准字段一起保存
    updated_unified_fields.extend(previous_unified_fields)

    return updated_unified_fields
