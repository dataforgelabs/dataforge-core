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
    many_join_list text[]);
	