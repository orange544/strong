MERGE (t:Teacher {teacher_id: 'T000123'})
SET t.teacher_name = '李明',
    t.title = '教授';

MERGE (p:Project {project_id: 'RP2024-00125'})
SET p.project_name = '面向异构教育数据的语义统一方法研究',
    p.project_level = '国家级';

MERGE (sf:SourceField {field_id: 'faculty_hr_db.teacher.teacher_name'});

MERGE (sc:SemanticConcept {concept_id: 'Teacher.Name'});

MATCH (t:Teacher {teacher_id: 'T000123'}), (p:Project {project_id: 'RP2024-00125'})
MERGE (t)-[:LEADS]->(p);

MATCH (sf:SourceField {field_id: 'faculty_hr_db.teacher.teacher_name'}), (sc:SemanticConcept {concept_id: 'Teacher.Name'})
MERGE (sf)-[:MAPS_TO]->(sc);