CREATE TABLE IF NOT EXISTS meta.enrichment_test
(
    enrichment_id INT PRIMARY KEY, 
    project_id int,
    expression text, 
    result json
);