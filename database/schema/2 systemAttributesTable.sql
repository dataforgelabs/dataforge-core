
CREATE TABLE IF NOT EXISTS meta.system_attribute
(
    system_attribute_id int ,
    refresh_type text[] COLLATE pg_catalog."default",
    table_type text[] COLLATE pg_catalog."default",
    ordinal integer NOT NULL,
    name text COLLATE pg_catalog."default",
    data_type text COLLATE pg_catalog."default",
    description text COLLATE pg_catalog."default",
    table_output_flag boolean NOT NULL DEFAULT true,
    table_alias char(1),
    display_name text,
    attribute_parameters json,
    CONSTRAINT system_attribute_pkey PRIMARY KEY (system_attribute_id),
    CONSTRAINT system_attribute_datatype FOREIGN KEY (data_type)
    REFERENCES meta.attribute_type (hive_type)

);