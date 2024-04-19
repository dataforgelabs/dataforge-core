
CREATE TABLE IF NOT EXISTS meta.enrichment
(
    enrichment_id integer NOT NULL DEFAULT nextval('meta.seq_enrichment'::regclass),
    parent_enrichment_id integer NULL,
    source_id integer NOT NULL,
    priority integer,
    name text COLLATE pg_catalog."default" NOT NULL,
    description text COLLATE pg_catalog."default" NULL,
    datatype text COLLATE pg_catalog."default",
    datatype_schema jsonb,
    cast_datatype text COLLATE pg_catalog."default",
    attribute_name text COLLATE pg_catalog."default",
    expression text COLLATE pg_catalog."default",
    expression_parsed text,
    rule_type_code character(1) COLLATE pg_catalog."default",
    validation_action_code char(1), -- F,W
    validation_type_code char(1), -- U (User created), C (Datatype cast), U (Uniqueness) 
    keep_current_flag boolean NOT NULL DEFAULT false,
    window_function_flag boolean NOT NULL DEFAULT false,
    unique_flag boolean NOT NULL DEFAULT false,
    active_flag boolean NOT NULL DEFAULT true,
    create_datetime timestamp without time zone NOT NULL DEFAULT now(),
    created_userid text COLLATE pg_catalog."default" NOT NULL DEFAULT "current_user"(),
    update_datetime timestamp without time zone,
    updated_userid text COLLATE pg_catalog."default",
    rule_template_id integer,
    CONSTRAINT pk_enrichment PRIMARY KEY (enrichment_id),
    CONSTRAINT uc_enrichment_attribute_name UNIQUE (source_id, attribute_name, active_flag),
    CONSTRAINT fk_enrichment_source_id FOREIGN KEY (source_id)
        REFERENCES meta.source (source_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT enrichment_rule_type_code_check CHECK (rule_type_code = ANY (ARRAY['V'::bpchar, 'E'::bpchar])),
    CONSTRAINT enrichment_datatype_fkey FOREIGN KEY (datatype) REFERENCES meta.attribute_type(hive_type),
    CONSTRAINT enrichment_cast_datatype_fkey FOREIGN KEY (datatype) REFERENCES meta.attribute_type(hive_type),
    CONSTRAINT enrichment_parent_enrichment_id_fkey FOREIGN KEY (parent_enrichment_id)
        REFERENCES meta.enrichment (enrichment_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_enrichment_source_id
    ON meta.enrichment USING btree
    (source_id)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS ix_enrichment_parent_enrichment_id
    ON meta.enrichment USING btree
    (parent_enrichment_id)
    TABLESPACE pg_default;

