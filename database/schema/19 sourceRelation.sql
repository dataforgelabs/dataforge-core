CREATE TABLE IF NOT EXISTS meta.source_relation(
    source_relation_id int DEFAULT nextval('meta.seq_source_relation'::regclass),
    relation_name text,
    source_id int,
    related_source_id int,
    expression text,
    source_cardinality text,
    related_source_cardinality text,
    expression_parsed text,
    active_flag boolean,
    create_datetime timestamp DEFAULT now(),
    created_userid text,
    update_datetime timestamp,
    updated_userid text,
    description text,
    primary_flag boolean,
    relation_template_id int,
    CONSTRAINT source_relation_pkey PRIMARY KEY (source_relation_id),
    CONSTRAINT source_relation_source_id_fkey FOREIGN KEY (source_id) REFERENCES meta.source (source_id),
    CONSTRAINT source_relation_related_source_id_fkey FOREIGN KEY (related_source_id) REFERENCES  meta.source (source_id),
    CONSTRAINT u_source_relation_name UNIQUE (source_id, related_source_id, relation_name)
);


CREATE INDEX IF NOT EXISTS ix_source_relation_source_id_active_flag ON meta.source_relation (source_id) WHERE active_flag ;
CREATE INDEX IF NOT EXISTS ix_source_relation_related_source_id_active_flag ON meta.source_relation (related_source_id) WHERE active_flag ;
