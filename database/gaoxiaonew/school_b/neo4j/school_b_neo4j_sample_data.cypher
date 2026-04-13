MERGE (f:Faculty {faculty_no: 'F000221'})
SET f.faculty_name = '周宁',
    f.rank = '副教授';

MERGE (g:Grant {grant_id: 'GA2024-016'})
SET g.grant_name = '多高校数据共享语义映射研究',
    g.grant_level = '省部级';

MERGE (sf:SourceField {field_id: 'faculty_research_db.faculty_member.faculty_name'});

MERGE (sc:SemanticConcept {concept_id: 'Teacher.Name'});

MATCH (f:Faculty {faculty_no: 'F000221'}), (g:Grant {grant_id: 'GA2024-016'})
MERGE (f)-[:LEADS]->(g);

MATCH (sf:SourceField {field_id: 'faculty_research_db.faculty_member.faculty_name'}), (sc:SemanticConcept {concept_id: 'Teacher.Name'})
MERGE (sf)-[:MAPS_TO]->(sc);