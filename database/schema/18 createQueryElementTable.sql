DROP TABLE IF EXISTS meta.query_element CASCADE;

CREATE TABLE meta.query_element (
    id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY , 
    type text,
    source_id int , -- this is source_id container of specific element
    expression text, 
    alias text, 
    attribute_id int, 
    parent_ids int[], 
    relation_ids int[],
    cte int,
    data_type text,
    container_source_id int, -- target source_id of the query execution, or sub_source_id for nested sub-source query
    container_source_ids int[] -- used for copy sub-source transits across multiple source containers
    );
	