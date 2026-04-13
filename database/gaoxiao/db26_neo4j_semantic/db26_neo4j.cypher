// Neo4j schema for DB26 semantic governance graph.

CREATE CONSTRAINT system_id_unique IF NOT EXISTS
FOR (n:System)
REQUIRE n.system_id IS UNIQUE;

CREATE CONSTRAINT database_id_unique IF NOT EXISTS
FOR (n:Database)
REQUIRE n.db_id IS UNIQUE;

CREATE CONSTRAINT table_id_unique IF NOT EXISTS
FOR (n:Table)
REQUIRE n.table_id IS UNIQUE;

CREATE CONSTRAINT field_id_unique IF NOT EXISTS
FOR (n:Field)
REQUIRE n.field_id IS UNIQUE;

CREATE CONSTRAINT business_concept_id_unique IF NOT EXISTS
FOR (n:BusinessConcept)
REQUIRE n.concept_id IS UNIQUE;

CREATE CONSTRAINT student_id_unique IF NOT EXISTS
FOR (n:Student)
REQUIRE n.student_id IS UNIQUE;

CREATE CONSTRAINT teacher_id_unique IF NOT EXISTS
FOR (n:Teacher)
REQUIRE n.teacher_id IS UNIQUE;

CREATE CONSTRAINT staff_id_unique IF NOT EXISTS
FOR (n:Staff)
REQUIRE n.staff_id IS UNIQUE;

CREATE CONSTRAINT project_id_unique IF NOT EXISTS
FOR (n:Project)
REQUIRE n.project_id IS UNIQUE;

CREATE CONSTRAINT asset_id_unique IF NOT EXISTS
FOR (n:Asset)
REQUIRE n.asset_id IS UNIQUE;

CREATE CONSTRAINT room_id_unique IF NOT EXISTS
FOR (n:DormRoom)
REQUIRE n.room_id IS UNIQUE;

CREATE CONSTRAINT account_id_unique IF NOT EXISTS
FOR (n:Account)
REQUIRE n.account_id IS UNIQUE;

CREATE CONSTRAINT course_code_unique IF NOT EXISTS
FOR (n:Course)
REQUIRE n.course_code IS UNIQUE;

CREATE INDEX field_name_index IF NOT EXISTS
FOR (n:Field)
ON (n.field_name);

CREATE INDEX field_source_index IF NOT EXISTS
FOR (n:Field)
ON (n.source_db, n.source_table);

CREATE INDEX concept_name_index IF NOT EXISTS
FOR (n:BusinessConcept)
ON (n.concept_name);
