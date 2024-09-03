
CREATE TABLE IF NOT EXISTS meta.raw_attribute(
    raw_attribute_id int DEFAULT nextval('meta.seq_raw_attribute'::regclass),
    source_id int,
    raw_attribute_name text NOT NULL,
    description text,
    column_normalized text,
    raw_metadata jsonb,
    last_input_id int,
    data_type text NOT NULL,
    version_number int,
    column_alias text,
    unique_flag boolean DEFAULT false,
    datatype_schema jsonb,
    update_datetime timestamp,
    updated_userid text,
    CONSTRAINT raw_attribute_pkey PRIMARY KEY (raw_attribute_id),
    CONSTRAINT raw_attribute_source_id_fkey FOREIGN KEY (source_id) REFERENCES meta.source(source_id),
    CONSTRAINT raw_attribute_data_type_fkey FOREIGN KEY (data_type) REFERENCES meta.attribute_type (hive_type),
    CONSTRAINT raw_attribute_column_alias UNIQUE(column_alias, source_id)
);

CREATE INDEX IF NOT EXISTS ix_raw_attribute_source_id  ON meta.raw_attribute (source_id);



