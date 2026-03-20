import json
from typing import List, Dict
from openai import OpenAI
from src.configs.config import (
    LLM_MAX_RETRIES,
    LLM_UNIFY_MAX_TOKENS,
    LLM_UNIFY_TIMEOUT_SEC,
)


class FieldSemanticAgent:
    def __init__(self, api_key: str, base_url: str, model_name: str):
        self.request_timeout = LLM_UNIFY_TIMEOUT_SEC
        self.max_retries = LLM_MAX_RETRIES
        client_kwargs = {
            "api_key": api_key,
            "base_url": base_url,
            "max_retries": self.max_retries,
        }
        if self.request_timeout is not None:
            client_kwargs["timeout"] = self.request_timeout
        self.client = OpenAI(**client_kwargs)
        self.model_name = model_name

    def _call_llm(self, prompt: str) -> List[Dict]:
        try:
            request_kwargs = {
                "model": self.model_name,
                "messages": [
                    {"role": "system", "content": "你是一个结构化数据语义聚合专家。"},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.2,
                "max_tokens": LLM_UNIFY_MAX_TOKENS,
            }
            if self.request_timeout is not None:
                request_kwargs["timeout"] = self.request_timeout

            response = self.client.chat.completions.create(**request_kwargs)
            content = response.choices[0].message.content.strip()

            if not content:
                raise RuntimeError("模型未返回任何内容")

            if content.startswith("```"):
                content = content.strip("`").replace("json", "").strip()

            return json.loads(content)

        except Exception as e:
            raise RuntimeError(f"LLM 聚合失败: {e}")

    def unify_within_domain(self, field_desc_list: List[Dict]) -> List[Dict]:
        """
        域内聚类：同一个数据库内部，不同表之间字段聚类。
        输入字段格式：
        {
            "db_name": "...",
            "table": "...",
            "field": "...",
            "description": "..."
        }
        输出统一格式：
        {
            "canonical_name": "...",
            "fields": ["数据库.表.字段", ...],
            "description": "..."
        }
        """
        json_input = json.dumps(field_desc_list, ensure_ascii=False, indent=2)

        prompt = f"""
你是一名数据库语义标准化专家。以下是同一个数据库中的字段及其语义描述：

{json_input}

你的任务是：
1. 比较同一数据库中不同表之间的字段；
2. 将语义相同或相近的字段分到同一组；
3. 同一张表内的字段不要分到同一组；
4. 每组最多包含两个字段；
5. 为每组生成一个统一英文名和英文描述。

请严格输出合法 JSON 数组，每个元素格式如下：
{{
  "canonical_name": "标准字段名（英文小写，用下划线连接）",
  "fields": ["数据库名.表名.字段名", "数据库名.表名.字段名"],
  "description": "这些字段的统一语义描述"
}}

注意：
- 只比较同一个数据库中不同表之间的字段；
- 同一张表内字段不要分组在一起；
- 只输出 JSON 数组；
- 不要输出解释；
- 不要输出 ```json。
"""
        return self._call_llm(prompt)

    def unify_across_domains(self, domain_items: List[Dict]) -> List[Dict]:
        """
        跨域聚类：对各域输出的候选语义单元再做聚类。
        输入格式：
        {
            "canonical_name": "...",
            "fields": [...],
            "description": "...",
            "db_name": "..."
        }
        """
        json_input = json.dumps(domain_items, ensure_ascii=False, indent=2)

        prompt = f"""
你是一名跨数据库语义标准化专家。以下是多个数据库中的候选字段语义单元：

{json_input}

你的任务是：
1. 比较不同数据库之间的候选字段语义单元；
2. 将语义相同或相近的项分到同一组；
3. 同一数据库内的项不要分到同一组；
4. 每组最多包含两个项；
5. 为每组生成统一英文名和英文描述。

请严格输出合法 JSON 数组，每个元素格式如下：
{{
  "canonical_name": "标准字段名（英文小写，用下划线连接）",
  "fields": ["数据库名.表名.字段名", "数据库名.表名.字段名"],
  "description": "这些字段的统一语义描述"
}}

注意：
- 允许跨数据库聚类；
- 同一数据库内的项不要分到同一组；
- 只输出 JSON 数组；
- 不要输出解释；
- 不要输出 ```json。
"""
        return self._call_llm(prompt)
