from typing import Any


def esc(value: Any) -> str:
    text = str(value)
    return text.replace("\\", "\\\\").replace("'", "\\'")


class KnowledgeGraphAgent:
    """
    拆分后的知识图谱生成逻辑：

    1. 域内图（Domain KG）
       - DataSource
       - ResourceFile（sample / field_descriptions / domain_unified）
       - Table
       - Attribute
       - EntityConcept
       - PropertyConcept

    2. 跨域对齐层（Alignment）
       - alignment_index.json（独立 JSON 文件）
       - alignment_cypher.json（独立 Cypher 文件）
       - StandardEntityConcept
       - StandardPropertyConcept
       - Attribute -> StandardPropertyConcept
       - EntityConcept -> StandardEntityConcept
       - Attribute <-> Attribute SAME_AS
    """

    def generate_domain_kg_cypher(
        self,
        run_record: dict[str, Any],
        db_name: str,
        tables_data: dict[str, list[str]],
        field_descs: list[dict[str, Any]],
        domain_unified: list[dict[str, Any]],
    ) -> list[str]:
        """
        为单个数据域生成域内知识图谱 Cypher。
        """
        cypher_list: list[str] = []
        db_esc = esc(db_name)

        domain = next((d for d in run_record.get("domains", []) if d["db_name"] == db_name), None)
        if domain is None:
            return cypher_list

        # =========================================================
        # 1. DataSource / ResourceFile（仅本域）
        # =========================================================
        cypher_list.append(f"MERGE (ds:DataSource {{name:'{db_esc}'}});")

        file_fields = [
            ("sample_file", "samples", domain.get("sample_chain_cid") or domain.get("samples_cid", "")),
            (
                "field_descriptions_file",
                "field_descriptions",
                domain.get("description_chain_cid") or domain.get("field_descriptions_cid", ""),
            ),
            ("domain_unified_file", "domain_unified", domain.get("domain_unified_cid", "")),
        ]

        for file_key, file_type, cid in file_fields:
            file_name = domain.get(file_key)
            if not file_name:
                continue

            file_esc = esc(file_name)
            cid_esc = esc(cid)

            cypher_list.append(
                f"""
MERGE (rf:ResourceFile {{name:'{file_esc}'}})
SET rf.file_type = '{esc(file_type)}',
    rf.cid = '{cid_esc}',
    rf.db_name = '{db_esc}'
WITH rf
MATCH (ds:DataSource {{name:'{db_esc}'}})
MERGE (rf)-[:GENERATED_FROM]->(ds);
""".strip()
            )

        # =========================================================
        # 2. Table / Attribute（仅本域）
        # =========================================================
        for table_name, fields_list in tables_data.items():
            table_esc = esc(table_name)

            cypher_list.append(
                f"""
MERGE (ds:DataSource {{name:'{db_esc}'}})
MERGE (t:Table {{name:'{table_esc}', db_name:'{db_esc}'}})
MERGE (t)-[:BELONGS_TO_DATABASE]->(ds);
""".strip()
            )

            for field_name in fields_list:
                field_esc = esc(field_name)
                cypher_list.append(
                    f"""
MERGE (a:Attribute {{name:'{field_esc}', table_name:'{table_esc}', db_name:'{db_esc}'}})
WITH a
MATCH (t:Table {{name:'{table_esc}', db_name:'{db_esc}'}})
MERGE (a)-[:BELONGS_TO_TABLE]->(t);
""".strip()
                )

        # =========================================================
        # 3. 域内 EntityConcept（表 -> 实体概念）
        # =========================================================
        for table_name in tables_data.keys():
            table_esc = esc(table_name)
            entity_name = f"{db_name}.{table_name}"
            entity_esc = esc(entity_name)

            cypher_list.append(
                f"""
MERGE (ec:EntityConcept {{name:'{entity_esc}'}})
SET ec.db_name = '{db_esc}',
    ec.local_name = '{table_esc}'
WITH ec
MATCH (t:Table {{name:'{table_esc}', db_name:'{db_esc}'}})
MERGE (t)-[:REPRESENTS_ENTITY]->(ec);
""".strip()
            )

        # =========================================================
        # 4. 域内 PropertyConcept
        # =========================================================
        for item in domain_unified:
            canonical_name = item.get("canonical_name", "")
            description = item.get("description", "")

            pc_name = f"{db_name}.{canonical_name}"
            pc_esc = esc(pc_name)
            desc_esc = esc(description)

            cypher_list.append(
                f"""
MERGE (pc:PropertyConcept {{name:'{pc_esc}'}})
SET pc.db_name = '{db_esc}',
    pc.local_name = '{esc(canonical_name)}',
    pc.description = '{desc_esc}';
""".strip()
            )

            # 域内属性概念 -> 原始字段
            for field_ref in item.get("fields", []):
                parts = field_ref.split(".")
                if len(parts) != 3:
                    continue

                ref_db, ref_table, ref_field = parts
                if ref_db != db_name:
                    continue

                ref_db_esc = esc(ref_db)
                ref_table_esc = esc(ref_table)
                ref_field_esc = esc(ref_field)

                cypher_list.append(
                    f"""
MATCH (a:Attribute {{name:'{ref_field_esc}', table_name:'{ref_table_esc}', db_name:'{ref_db_esc}'}})
MATCH (pc:PropertyConcept {{name:'{pc_esc}'}})
MERGE (a)-[:REPRESENTS_PROPERTY]->(pc);
""".strip()
                )

            # 域内属性概念 -> 实体概念
            if item.get("fields"):
                parts = item["fields"][0].split(".")
                if len(parts) == 3:
                    ref_db, ref_table, _ = parts
                    if ref_db == db_name:
                        entity_name = f"{ref_db}.{ref_table}"
                        entity_esc = esc(entity_name)

                        cypher_list.append(
                            f"""
MATCH (pc:PropertyConcept {{name:'{pc_esc}'}})
MATCH (ec:EntityConcept {{name:'{entity_esc}'}})
MERGE (pc)-[:BELONGS_TO_ENTITY]->(ec);
""".strip()
                        )

        # =========================================================
        # 5. ResourceFile -> Attribute / PropertyConcept（仅本域）
        # =========================================================
        desc_file = domain.get("field_descriptions_file")
        if desc_file:
            desc_file_esc = esc(desc_file)
            for item in field_descs:
                table_name = item.get("table", "")
                field_name = item.get("field", "")
                table_esc = esc(table_name)
                field_esc = esc(field_name)

                cypher_list.append(
                    f"""
MATCH (rf:ResourceFile {{name:'{desc_file_esc}'}})
MATCH (a:Attribute {{name:'{field_esc}', table_name:'{table_esc}', db_name:'{db_esc}'}})
MERGE (rf)-[:DESCRIBES_ATTRIBUTE]->(a);
""".strip()
                )

        domain_unified_file = domain.get("domain_unified_file")
        if domain_unified_file:
            file_esc = esc(domain_unified_file)
            for item in domain_unified:
                canonical_name = item.get("canonical_name", "")
                pc_name = f"{db_name}.{canonical_name}"
                pc_esc = esc(pc_name)

                cypher_list.append(
                    f"""
MATCH (rf:ResourceFile {{name:'{file_esc}'}})
MATCH (pc:PropertyConcept {{name:'{pc_esc}'}})
MERGE (rf)-[:CONTAINS_PROPERTY_CONCEPT]->(pc);
""".strip()
                )

        return cypher_list

    def generate_alignment_index(
        self,
        unified_fields: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        从跨域 unified_fields 中生成独立 alignment index。
        """
        relations: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()

        for item in unified_fields:
            canonical_name = item.get("canonical_name", "")
            description = item.get("description", "")
            fields = item.get("fields", []) or []

            parsed_fields: list[tuple[str, str, str]] = []
            for field_ref in fields:
                parts = field_ref.split(".")
                if len(parts) != 3:
                    continue
                parsed_fields.append((parts[0], parts[1], parts[2]))

            for i in range(len(parsed_fields)):
                for j in range(i + 1, len(parsed_fields)):
                    src_db, src_table, src_field = parsed_fields[i]
                    tgt_db, tgt_table, tgt_field = parsed_fields[j]

                    if src_db == tgt_db:
                        continue

                    left = f"{src_db}.{src_table}.{src_field}"
                    right = f"{tgt_db}.{tgt_table}.{tgt_field}"
                    a, b = sorted([left, right])
                    dedup_key = (a, b, canonical_name)
                    if dedup_key in seen:
                        continue
                    seen.add(dedup_key)

                    relations.append(
                        {
                            "relation_type": "SAME_AS",
                            "canonical_name": canonical_name,
                            "description": description,
                            "score": 1.0,
                            "source_domain": src_db,
                            "source_table": src_table,
                            "source_field": src_field,
                            "target_domain": tgt_db,
                            "target_table": tgt_table,
                            "target_field": tgt_field,
                        }
                    )

        return relations

    def generate_alignment_cypher(
        self,
        run_record: dict[str, Any],
        db_data: dict[str, dict[str, list[str]]],
        unified_fields: list[dict[str, Any]],
        alignment_index: list[dict[str, Any]],
    ) -> list[str]:
        """
        生成跨域对齐索引 Cypher。
        """
        cypher_list: list[str] = []

        # =========================================================
        # 1. alignment / unified 资源文件
        # =========================================================
        if run_record.get("alignment_index_file"):
            alignment_file = esc(run_record["alignment_index_file"])
            alignment_cid = esc(run_record.get("alignment_index_cid", ""))
            cypher_list.append(
                f"""
MERGE (rf:ResourceFile {{name:'{alignment_file}'}})
SET rf.file_type = 'alignment_index',
    rf.cid = '{alignment_cid}';
""".strip()
            )

        if run_record.get("unified_fields_file"):
            uf_file = esc(run_record["unified_fields_file"])
            uf_cid = esc(run_record.get("unified_fields_cid", ""))
            cypher_list.append(
                f"""
MERGE (rf:ResourceFile {{name:'{uf_file}'}})
SET rf.file_type = 'cross_domain_unified',
    rf.cid = '{uf_cid}';
""".strip()
            )

        # =========================================================
        # 2. StandardPropertyConcept
        # =========================================================
        for item in unified_fields:
            canonical_name = item.get("canonical_name", "")
            description = item.get("description", "")

            spc_esc = esc(canonical_name)
            desc_esc = esc(description)

            cypher_list.append(
                f"""
MERGE (spc:StandardPropertyConcept {{name:'{spc_esc}'}})
SET spc.description = '{desc_esc}';
""".strip()
            )

            for field_ref in item.get("fields", []):
                parts = field_ref.split(".")
                if len(parts) != 3:
                    continue

                ref_db, ref_table, ref_field = parts
                ref_db_esc = esc(ref_db)
                ref_table_esc = esc(ref_table)
                ref_field_esc = esc(ref_field)

                cypher_list.append(
                    f"""
MATCH (a:Attribute {{name:'{ref_field_esc}', table_name:'{ref_table_esc}', db_name:'{ref_db_esc}'}})
MATCH (spc:StandardPropertyConcept {{name:'{spc_esc}'}})
MERGE (a)-[:IS_STANDARDIZED_AS]->(spc);
""".strip()
                )

            if run_record.get("unified_fields_file"):
                uf_file = esc(run_record["unified_fields_file"])
                cypher_list.append(
                    f"""
MATCH (rf:ResourceFile {{name:'{uf_file}'}})
MATCH (spc:StandardPropertyConcept {{name:'{spc_esc}'}})
MERGE (rf)-[:CONTAINS_STANDARD_PROPERTY]->(spc);
""".strip()
                )

        # =========================================================
        # 3. StandardEntityConcept（按表名跨域统一）
        # =========================================================
        seen_standard_entities = set()
        for db_name, tables_data in db_data.items():
            for table_name in tables_data:
                standard_entity = table_name.lower()
                sec_esc = esc(standard_entity)

                if standard_entity not in seen_standard_entities:
                    seen_standard_entities.add(standard_entity)
                    cypher_list.append(
                        f"""
MERGE (sec:StandardEntityConcept {{name:'{sec_esc}'}})
SET sec.description = '跨域统一实体概念：{sec_esc}';
""".strip()
                    )

                domain_entity_name = f"{db_name}.{table_name}"
                domain_entity_esc = esc(domain_entity_name)

                cypher_list.append(
                    f"""
MATCH (ec:EntityConcept {{name:'{domain_entity_esc}'}})
MATCH (sec:StandardEntityConcept {{name:'{sec_esc}'}})
MERGE (ec)-[:STANDARDIZED_AS]->(sec);
""".strip()
                )

        # =========================================================
        # 4. alignment_index -> SAME_AS 边
        # =========================================================
        for rel in alignment_index:
            canonical_name = esc(rel.get("canonical_name", ""))
            score = rel.get("score", 1.0)

            src_db = esc(rel["source_domain"])
            src_table = esc(rel["source_table"])
            src_field = esc(rel["source_field"])

            tgt_db = esc(rel["target_domain"])
            tgt_table = esc(rel["target_table"])
            tgt_field = esc(rel["target_field"])

            cypher_list.append(
                f"""
MATCH (src:Attribute {{name:'{src_field}', table_name:'{src_table}', db_name:'{src_db}'}})
MATCH (tgt:Attribute {{name:'{tgt_field}', table_name:'{tgt_table}', db_name:'{tgt_db}'}})
MERGE (src)-[r:SAME_AS]->(tgt)
SET r.score = {score},
    r.canonical_name = '{canonical_name}';
""".strip()
            )

        return cypher_list

    def generate_cypher(
        self,
        run_record: dict[str, Any],
        db_data: dict[str, dict[str, list[str]]],
        domain_field_desc_map: dict[str, list[dict[str, Any]]],
        domain_unified_map: dict[str, list[dict[str, Any]]],
        unified_fields: list[dict[str, Any]],
    ) -> list[str]:
        """
        兼容旧接口：仍可返回“域内图 + 对齐图”的总 Cypher。
        """
        all_cypher: list[str] = []

        for db_name, tables_data in db_data.items():
            all_cypher.extend(
                self.generate_domain_kg_cypher(
                    run_record=run_record,
                    db_name=db_name,
                    tables_data=tables_data,
                    field_descs=domain_field_desc_map.get(db_name, []),
                    domain_unified=domain_unified_map.get(db_name, []),
                )
            )

        alignment_index = self.generate_alignment_index(unified_fields)
        all_cypher.extend(
            self.generate_alignment_cypher(
                run_record=run_record,
                db_data=db_data,
                unified_fields=unified_fields,
                alignment_index=alignment_index,
            )
        )
        return all_cypher