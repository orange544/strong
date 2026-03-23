# Unified Pipeline Contract (FieldUnit-first)

## Scope
This contract defines the target behavior for the multi-database semantic pipeline.
The pipeline must treat `FieldUnit` as the canonical intermediate representation and keep
legacy compatibility fields for existing callers.

## Acceptance Criteria
1. The pipeline accepts heterogeneous sources from `DB_SOURCES_JSON`.
2. Sampling is executed through unified adapters, not `DatabaseAgent`-specific logic.
3. Sample artifacts include schema/version metadata and compatibility keys.
4. Main pipeline (`main.py --mode all`) and domain-share pipeline both use the same
   FieldUnit-first extraction path.
5. Unsupported drivers fail fast with explicit supported-driver messages.

## Canonical Field Sample Schema

Each sample item must contain:

- `schema_version: str` (`field-sample/1.0`)
- `pipeline_target_version: str`
- `source_name: str`
- `database_type: str`
- `container_name: str`
- `field_path: str`
- `original_field: str`
- `field_origin: str`
- `logical_type: str`
- `samples: list[str]`

Compatibility fields kept for existing code paths:

- `db_name: str` (same as `source_name`)
- `table: str` (same as `container_name`)
- `field: str` (same as `field_path`)
- `type: str` (same as `logical_type`)
- `field_ref: str` (`<db>.<container>.<field_path>`)

## Compatibility Rules
1. Existing components consuming `{table, field, samples}` continue to work.
2. New components should consume canonical keys first and treat compatibility keys as legacy.
3. `DatabaseAgent` remains unchanged and may still be used by legacy modules.
