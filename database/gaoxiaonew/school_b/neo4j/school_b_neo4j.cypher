CREATE CONSTRAINT faculty_no_unique IF NOT EXISTS
FOR (n:Faculty)
REQUIRE n.faculty_no IS UNIQUE;

CREATE CONSTRAINT grant_id_unique IF NOT EXISTS
FOR (n:Grant)
REQUIRE n.grant_id IS UNIQUE;

CREATE CONSTRAINT source_field_id_unique IF NOT EXISTS
FOR (n:SourceField)
REQUIRE n.field_id IS UNIQUE;

CREATE CONSTRAINT semantic_concept_id_unique IF NOT EXISTS
FOR (n:SemanticConcept)
REQUIRE n.concept_id IS UNIQUE;

CREATE INDEX source_field_id_index IF NOT EXISTS
FOR (n:SourceField)
ON (n.field_id);

CREATE INDEX semantic_concept_id_index IF NOT EXISTS
FOR (n:SemanticConcept)
ON (n.concept_id);