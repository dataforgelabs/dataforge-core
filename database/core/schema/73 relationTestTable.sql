CREATE TABLE IF NOT EXISTS meta.relation_test
(
    source_relation_id INT PRIMARY KEY, 
    project_id int,
    expression text, 
    result json
);