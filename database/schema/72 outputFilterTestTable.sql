CREATE TABLE IF NOT EXISTS meta.output_filter_test
(
    output_source_id INT PRIMARY KEY, 
    project_id int,
    expression text, 
    result json
);