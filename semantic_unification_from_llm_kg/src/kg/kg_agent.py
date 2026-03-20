from typing import Any


def esc(value: Any) -> str:
    text = str(value)
    return text.replace("\\", "\\\\").replace("'", "\\'")


class KnowledgeGraphAgent:
    """
    新版知识图谱生成逻辑：

    1. 资源层
       - DataSource
       - ResourceFile

    2. 结构层
       - Table
       - Attribute

    3. 域内概念层
       - EntityConcept
       - PropertyConcept

    4. 跨域标准层
       - StandardEntityConcept
       - StandardPropertyConcept

    5. 关系
       - Table -> DataSource
       - Attribute -> Table
       - Table -> EntityConcept
       - Attribute -> PropertyConcept
       - PropertyConcept -> EntityConcept
       - EntityConcept -> StandardEntityConcept
       - PropertyConcept -> StandardPropertyConcept
       - ResourceFile -> Attribute / Concept / StandardConcept
    """

    def generate_cypher(
        self,
        run_record: dict[str, Any],
        db_data: dict[str, dict[str, list[str]]],
        domain_field_desc_map: dict[str, list[dict[str, Any]]],
        domain_unified_map: dict[str, list[dict[str, Any]]],
        unified_fields: list[dict[str, Any]],
    ) -> list[str]:
        cypher_list: list[str] = []

        # =========================================================
        # 1. DataSource / ResourceFile
        # =========================================================
        for domain in run_record.get("domains", []):
            db_name = domain["db_name"]
            db_esc = esc(db_name)

            cypher_list.append(
                f"MERGE (ds:DataSource {{name:'{db_esc}'}});"
            )

            file_fields = [
                ("sample_file", "samples", domain.get("samples_cid", "")),
                ("field_descriptions_file", "field_descriptions", domain.get("field_descriptions_cid", "")),
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

        # 最终跨域统一文件
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

        if run_record.get("cypher_file"):
            cypher_file = esc(run_record["cypher_file"])
            cypher_cid = esc(run_record.get("cypher_cid", ""))
            cypher_list.append(
                f"""
MERGE (rf:ResourceFile {{name:'{cypher_file}'}})
SET rf.file_type = 'cypher',
    rf.cid = '{cypher_cid}';
""".strip()
            )

        # =========================================================
        # 2. Table / Attribute
        # =========================================================
        for db_name, tables_data in db_data.items():
            db_esc = esc(db_name)

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
        #    当前采用“表名作为域内实体概念名”
        # =========================================================
        for db_name, tables_data in db_data.items():
            db_esc = esc(db_name)

            for table_name in tables_data:
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
        #    域内聚类结果中每一组形成一个属性概念
        # =========================================================
        for db_name, domain_unified in domain_unified_map.items():
            db_esc = esc(db_name)

            for item in domain_unified:
                canonical_name = item.get("canonical_name", "")
                description = item.get("description", "")

                # 域内属性概念名：db.canonical_name
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
                    # 约定格式：db.table.field
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
MATCH (pc:PropertyConcept {{name:'{pc_esc}'}})
MERGE (a)-[:REPRESENTS_PROPERTY]->(pc);
""".strip()
                    )

                # 域内属性概念 -> 实体概念
                # 这里默认用 fields 中第一个字段所属表作为该属性概念的实体概念
                if item.get("fields"):
                    parts = item["fields"][0].split(".")
                    if len(parts) == 3:
                        ref_db, ref_table, _ = parts
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
        # 5. 跨域 StandardPropertyConcept
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

                # 原始字段 -> 跨域标准属性概念
                cypher_list.append(
                    f"""
MATCH (a:Attribute {{name:'{ref_field_esc}', table_name:'{ref_table_esc}', db_name:'{ref_db_esc}'}})
MATCH (spc:StandardPropertyConcept {{name:'{spc_esc}'}})
MERGE (a)-[:IS_STANDARDIZED_AS]->(spc);
""".strip()
                )

                # 域内属性概念 -> 跨域标准属性概念
                domain_pc_name = f"{ref_db}.{canonical_name}"
                domain_pc_esc = esc(domain_pc_name)

                cypher_list.append(
                    f"""
MERGE (pc:PropertyConcept {{name:'{domain_pc_esc}'}})
WITH pc
MATCH (spc:StandardPropertyConcept {{name:'{spc_esc}'}})
MERGE (pc)-[:STANDARDIZED_AS]->(spc);
""".strip()
                )

                # 最终统一文件 -> 标准属性概念
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
        # 6. 跨域 StandardEntityConcept
        #    当前简单策略：按“表名”跨域统一实体概念
        #    例如 IMDB.movie / TMDB.movie -> StandardEntityConcept(movie)
        # =========================================================
        seen_standard_entities = set()

        for db_name, tables_data in db_data.items():
            for table_name in tables_data:
                standard_entity = table_name.lower()
                if standard_entity not in seen_standard_entities:
                    seen_standard_entities.add(standard_entity)
                    sec_esc = esc(standard_entity)
                    cypher_list.append(
                        f"""
MERGE (sec:StandardEntityConcept {{name:'{sec_esc}'}})
SET sec.description = '跨域统一实体概念：{sec_esc}';
""".strip()
                    )

                domain_entity_name = f"{db_name}.{table_name}"
                domain_entity_esc = esc(domain_entity_name)
                sec_esc = esc(standard_entity)

                cypher_list.append(
                    f"""
MATCH (ec:EntityConcept {{name:'{domain_entity_esc}'}})
MATCH (sec:StandardEntityConcept {{name:'{sec_esc}'}})
MERGE (ec)-[:STANDARDIZED_AS]->(sec);
""".strip()
                )

        # =========================================================
        # 7. 资源文件 -> Attribute / Concept / StandardConcept
        # =========================================================
        for db_name, field_descs in domain_field_desc_map.items():
            domain = next((d for d in run_record.get("domains", []) if d["db_name"] == db_name), None)
            if not domain:
                continue

            desc_file = domain.get("field_descriptions_file")
            if not desc_file:
                continue

            desc_file_esc = esc(desc_file)

            for item in field_descs:
                table_name = item.get("table", "")
                field_name = item.get("field", "")
                table_esc = esc(table_name)
                field_esc = esc(field_name)
                db_esc = esc(db_name)

                cypher_list.append(
                    f"""
MATCH (rf:ResourceFile {{name:'{desc_file_esc}'}})
MATCH (a:Attribute {{name:'{field_esc}', table_name:'{table_esc}', db_name:'{db_esc}'}})
MERGE (rf)-[:DESCRIBES_ATTRIBUTE]->(a);
""".strip()
                )

        # domain_unified 文件 -> PropertyConcept
        for domain in run_record.get("domains", []):
            db_name = domain["db_name"]
            db_esc = esc(db_name)
            domain_unified_file = domain.get("domain_unified_file")
            if not domain_unified_file:
                continue

            file_esc = esc(domain_unified_file)
            domain_unified = domain_unified_map.get(db_name, [])

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
