CREATE CONSTRAINT teacher_id_unique IF NOT EXISTS
FOR (n:Teacher)
REQUIRE n.teacher_id IS UNIQUE;

CREATE CONSTRAINT project_id_unique IF NOT EXISTS
FOR (n:Project)
REQUIRE n.project_id IS UNIQUE;

CREATE CONSTRAINT source_field_id_unique IF NOT EXISTS
FOR (n:SourceField)
REQUIRE n.field_id IS UNIQUE;

CREATE CONSTRAINT semantic_concept_id_unique IF NOT EXISTS
FOR (n:SemanticConcept)
REQUIRE n.concept_id IS UNIQUE;

CREATE INDEX source_field_name_index IF NOT EXISTS
FOR (n:SourceField)
ON (n.field_id);

CREATE INDEX semantic_concept_name_index IF NOT EXISTS
FOR (n:SemanticConcept)
ON (n.concept_id);