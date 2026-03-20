import os
from typing import Dict
from openai import OpenAI


class FieldDescriptionAgent:
    def __init__(self, api_key: str, base_url: str, model_name: str):
        raw_timeout = float(os.getenv("LLM_REQUEST_TIMEOUT_SEC", "45"))
        self.request_timeout = None if raw_timeout <= 0 else raw_timeout
        self.max_retries = int(os.getenv("LLM_MAX_RETRIES", "1"))
        client_kwargs = {
            "api_key": api_key,
            "base_url": base_url,
            "max_retries": self.max_retries,
        }
        if self.request_timeout is not None:
            client_kwargs["timeout"] = self.request_timeout
        self.client = OpenAI(**client_kwargs)
        self.model_name = model_name

    def generate_description(self, field_json: Dict) -> Dict:
        """
        输入：sample_field 输出的 dict
        输出：包含 table / field / description 的 dict
        """
        field = field_json["field"]
        samples = ", ".join(map(str, field_json["samples"]))

        prompt = f"""
请你扮演一个数据库字段语义专家。该数据库是有关电影信息的数据库，
字段名一般是能表明样本意义的英文名称或者英文名称的首字母缩写，请你根据字段名和样本值，并结合现实电影信息的
实际情况与应用，对每一个字段生成简要英文描述。

字段名: {field}
样本值: {samples}

请输出一句简短英文描述。
"""

        request_kwargs = {
            "model": self.model_name,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "你是一个数据库字段语义解释助手。"
                        "你只能输出最终一句英文描述。"
                    "禁止输出任何思考过程、分析过程、推理过程、标签或额外解释。"
                 ),
                },
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.2,
        }
        if self.request_timeout is not None:
            request_kwargs["timeout"] = self.request_timeout

        response = self.client.chat.completions.create(
            **request_kwargs,
        )
        raw_text = response.choices[0].message.content or ""

        if "</think>" in raw_text:
            description = raw_text.split("</think>", 1)[1].strip()
        else:
            description = raw_text.strip()
        return {
            "table": field_json["table"],
            "field": field,
            "description": description
        }  