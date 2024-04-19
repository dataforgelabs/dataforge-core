CREATE TABLE IF NOT EXISTS meta.output_source_column
(
    output_source_column_id integer NOT NULL DEFAULT nextval('meta.seq_output_source_column'::regclass),
    output_source_id integer NOT NULL,
    output_column_id integer NOT NULL,
    expression text,
    datatype text,
    precision int,
    scale int,
    type text,
    enrichment_id int,
    raw_attribute_id int,
    system_attribute_id int,
    source_relation_ids int[],
    keys text, -- expression struct keys as bicycle.color from expression [This].store.bicycle.color
    create_datetime timestamp without time zone NOT NULL DEFAULT now(),
    created_userid text COLLATE pg_catalog."default" NOT NULL DEFAULT "current_user"(),
    update_datetime timestamp without time zone,
    updated_userid text COLLATE pg_catalog."default",
    aggregate text,
    aggregate_distinct_flag boolean,
    ipu_weight int,
    CONSTRAINT pk_output_source_column PRIMARY KEY (output_source_column_id),
    CONSTRAINT fk_output_source_column_output_column_id FOREIGN KEY (output_column_id)
        REFERENCES meta.output_column (output_column_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_output_source_column_output_source_id FOREIGN KEY (output_source_id)
        REFERENCES meta.output_source (output_source_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_output_source_column_enrichment_id FOREIGN KEY (enrichment_id) 
     REFERENCES meta.enrichment(enrichment_id) ON DELETE CASCADE
);



CREATE UNIQUE INDEX IF NOT EXISTS ux_output_source_column_output_source_id_output_column_id
    ON meta.output_source_column USING btree
    (output_source_id, output_column_id)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS ix_output_source_column_enrichment_id 
ON meta.output_source_column (enrichment_id)
WHERE type = 'enrichment';

CREATE INDEX IF NOT EXISTS ix_output_source_column_system_attribute_id  
ON meta.output_source_column (system_attribute_id )
WHERE type = 'system';

CREATE INDEX IF NOT EXISTS ix_output_source_column_raw_attribute_id 
ON meta.output_source_column (raw_attribute_id )
WHERE type = 'raw';