--Built: Wed Apr 17 06:01:43 PDT 2024
CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS log;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'parameter_map') THEN
        DROP TYPE parameter_map CASCADE; 
    END IF;
       CREATE TYPE parameter_map  AS (type text, raw_attribute_id int, enrichment_id int, system_attribute_id int, source_id int, error text, datatype text, datatype_schema jsonb);

END$$;--drop legacy functions
DO $$ 
DECLARE v_sql text;
BEGIN
FOR v_sql IN SELECT format('DROP FUNCTION IF EXISTS %I.%I(%s) CASCADE', ns.nspname, p.proname, oidvectortypes(p.proargtypes)) 
    FROM pg_proc p INNER JOIN pg_namespace ns ON (p.pronamespace = ns.oid)
    WHERE ns.nspname IN ('meta','sparky')
    AND p.proname NOT LIKE 'trg_%' -- skip trigger functions
    AND prokind = 'f' -- functions only
    LOOP
    EXECUTE v_sql;
    END LOOP;
END $$;DROP TABLE IF EXISTS  meta.attribute_type CASCADE;
CREATE TABLE IF NOT EXISTS meta.attribute_type(
    hive_type text PRIMARY KEY, -- our reference data type
    hive_ddl_type text,
    spark_type text[],
    complex_flag boolean
);


INSERT INTO meta.attribute_type(hive_type, hive_ddl_type, spark_type, complex_flag)  VALUES
('string','string','{StringType}',false),
('decimal','decimal(38,12)','{DecimalType}',false),
('timestamp','timestamp','{TimestampType}',false),
('boolean','boolean','{BooleanType}',false),
('int','integer','{ByteType,ShortType,IntegerType}',false),
('long','long','{LongType}',false),
('float','float','{FloatType}',false),
('double','double','{DoubleType}',false),
('struct','struct','{StructType}',true),
('array','array','{ArrayType}',true),
('date','date','{DateType}',false);CREATE SEQUENCE IF NOT EXISTS log.seq_log;
CREATE SEQUENCE IF NOT EXISTS meta.seq_enrichment;
CREATE SEQUENCE IF NOT EXISTS meta.seq_raw_attribute;
CREATE SEQUENCE IF NOT EXISTS meta.seq_system_attribute;
CREATE SEQUENCE IF NOT EXISTS meta.seq_query_element;
CREATE SEQUENCE IF NOT EXISTS meta.seq_source_relation;
CREATE SEQUENCE IF NOT EXISTS meta.seq_source;
CREATE SEQUENCE IF NOT EXISTS meta.seq_output;
CREATE SEQUENCE IF NOT EXISTS meta.seq_output_source;
CREATE SEQUENCE IF NOT EXISTS meta.seq_output_column;
CREATE SEQUENCE IF NOT EXISTS meta.seq_output_source_column;
CREATE TABLE IF NOT EXISTS meta.project
(
    project_id integer NOT NULL GENERATED ALWAYS AS IDENTITY,
    name text NOT NULL,
    description text,
    default_flag boolean NOT NULL DEFAULT (false),
    max_imports int NOT NULL DEFAULT 50,
    schema_name text NOT NULL,
    disable_ingestion_flag boolean DEFAULT false,
    lock_flag boolean DEFAULT false,
    create_datetime timestamp without time zone NOT NULL DEFAULT now(),
    created_userid text,
    update_datetime timestamp without time zone,
    updated_userid text,
    CONSTRAINT pk_project PRIMARY KEY (project_id),
    CONSTRAINT ux_project_name UNIQUE (name),
    CONSTRAINT ux_project_schema_name UNIQUE (schema_name)
);

DO $$
DECLARE
    v_schema text;
BEGIN
IF NOT EXISTS(SELECT 1 FROM meta.project) THEN -- insert default project record with project_id = 1
    INSERT INTO meta.project (name, description, default_flag,created_userid, schema_name)
      VALUES('Default', 'Default project automatically created by system', true, 'system', 'default');
END IF;   
END;
$$;  


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

);CREATE TABLE IF NOT EXISTS meta.source
(
    source_id integer NOT NULL DEFAULT nextval('meta.seq_source'::regclass),
    source_name text COLLATE pg_catalog."default" NOT NULL,
    source_description text COLLATE pg_catalog."default" NOT NULL,
    active_flag boolean NOT NULL,
    ignore_failed_validations_flag boolean,
    create_datetime timestamp without time zone NOT NULL DEFAULT now(),
    ingestion_parameters jsonb,
    created_userid text COLLATE pg_catalog."default",
    update_datetime timestamp without time zone DEFAULT now(),
    parsing_parameters jsonb,
    cdc_refresh_parameters jsonb,
    updated_userid text COLLATE pg_catalog."default",
    schedule_id int,
    alert_parameters jsonb,
    file_type text,
    refresh_type text,
    connection_id int,
    connection_type text,
    initiation_type text,
    loopback_output_id int,
    cost_parameters jsonb,
    parser text,
    source_template_id int,
    group_id int,
    hub_view_name text,
    hub_table_size bigint,
    process_configuration_id int,
    custom_ingest_cluster_configuration_id int,
    custom_parse_cluster_configuration_id int,
    cleanup_configuration_id int,
    project_id int NOT NULL DEFAULT 1,
    processing_type text,
    ipu_rules_weight int,
    CONSTRAINT pk_source PRIMARY KEY (source_id),
    CONSTRAINT ux_source_project_source_name UNIQUE (project_id, source_name),
    CONSTRAINT ux_source_project_hub_view_name UNIQUE (project_id, hub_view_name),
    CONSTRAINT fk_source_project FOREIGN KEY (project_id) REFERENCES meta.project (project_id)
);

CREATE TABLE IF NOT EXISTS meta.output
(
    output_id integer NOT NULL DEFAULT nextval('meta.seq_output'::regclass),
    output_type text COLLATE pg_catalog."default" NOT NULL,
    output_name text COLLATE pg_catalog."default" NOT NULL,
    active_flag boolean NOT NULL,
    created_userid text COLLATE pg_catalog."default" NOT NULL DEFAULT "current_user"(),
    create_datetime timestamp without time zone NOT NULL DEFAULT now(),
    update_datetime timestamp without time zone,
    output_package_parameters jsonb,
    updated_userid text COLLATE pg_catalog."default",
    retention_parameters jsonb,
    output_description text COLLATE pg_catalog."default",
    output_sub_type text COLLATE pg_catalog."default",
    connection_id int,
    alert_parameters jsonb,
    post_output_type text,
    custom_post_output_cluster_configuration_id int,
    group_id int,
    output_template_id int,
    project_id int NOT NULL DEFAULT 1,
    CONSTRAINT pk_output PRIMARY KEY (output_id),
    CONSTRAINT ux_output_project_output_name UNIQUE (project_id, output_name),
    CONSTRAINT fk_output_project FOREIGN KEY (project_id) REFERENCES meta.project (project_id)
);




CREATE TABLE IF NOT EXISTS meta.output_column
(
    output_column_id integer NOT NULL DEFAULT nextval('meta.seq_output_column'::regclass),
    output_id integer NOT NULL,
    position integer NOT NULL,
    name text COLLATE pg_catalog."default" NOT NULL,
    datatype text COLLATE pg_catalog."default" DEFAULT 'text'::text,
    precision smallint,
    scale smallint,
    max_length smallint,
    created_userid text COLLATE pg_catalog."default" NOT NULL DEFAULT "current_user"(),
    create_datetime timestamp without time zone NOT NULL DEFAULT now(),
    updated_userid text COLLATE pg_catalog."default",
    update_datetime timestamp without time zone,
    description text COLLATE pg_catalog."default",
    partition_ordinal integer,
    zorder_ordinal integer,
    CONSTRAINT pk_output_column PRIMARY KEY (output_column_id),
    CONSTRAINT fk_output_column_output_id FOREIGN KEY (output_id)
        REFERENCES meta.output (output_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT ck_output_column_precision CHECK ("precision" >= 0),
    CONSTRAINT ck_output_column_scale CHECK (scale >= 0),
    CONSTRAINT ux_output_column_name UNIQUE (output_id, name)
);

CREATE INDEX IF NOT EXISTS ux_output_column_output_id_position
    ON meta.output_column USING btree
    (output_id, "position")
    TABLESPACE pg_default;
DROP TABLE IF EXISTS meta.aggregate CASCADE;
CREATE TABLE IF NOT EXISTS meta.aggregate (
    aggregate_name TEXT PRIMARY KEY,
    description    TEXT,
    numeric_flag   BOOLEAN,
    data_type      TEXT
);

TRUNCATE TABLE meta.aggregate;
INSERT INTO meta.aggregate(aggregate_name, description, numeric_flag, data_type)
VALUES ('any', 'Returns true if at least one value of `expr` is true.', FALSE, 'boolean'),
    ('approx_count_distinct', 'Returns the estimated cardinality by HyperLogLog++', FALSE, 'long'),
('approx_percentile', 'Returns the approximate `percentile` of the numeric column `col` which is the smallest value in the ordered `col` values (sorted from least to greatest) such that no more than `percentage` of `col` values is less than the value or equal to that value.', TRUE, 'decimal'),
    ('avg', 'Returns the mean calculated from values of a group.', TRUE, 'double'),
    ('bit_or', 'Returns the bitwise OR of all non-null input values, or null if none.', FALSE, 'boolean'),
    ('bit_xor', 'Returns the bitwise XOR of all non-null input values, or null if none.', FALSE, 'boolean'),
    ('bool_and', 'Returns true if all values of `expr` are true.', FALSE, 'boolean'),
    ('bool_or', 'Returns true if at least one value of `expr` is true.', FALSE, 'boolean'),
    ('collect_list', 'Collects and returns a list of non-unique elements', FALSE, 'array'),
    ('collect_set', 'Collects and returns a set of unique elements', FALSE, 'array'),
    ('corr', 'Returns Pearson coefficient of correlation between a set of number pairs.', TRUE, 'numeric'),
    ('count', 'Returns the number of rows for which the column expression is non-null.', FALSE, 'long'),
    ('count_if', 'Returns the number of `TRUE` values for the expression.', FALSE, 'long'),
('count_min_sketch', 'Returns a count-min sketch of a column with the given esp, confidence and seed. The result is an array of bytes, which can be deserialized to a `CountMinSketch` before usage. Count-min sketch is a probabilistic data structure used for cardinality estimation using sub-linear space.', FALSE, 'string'),
    ('covar_pop', 'Returns the population covariance of a set of number pairs.', TRUE, 'decimal'),
    ('covar_samp', 'Returns the sample covariance of a set of number pairs.', TRUE, 'decimal'),
    ('every', 'Returns true if all values of `expr` are true.', FALSE, 'boolean'),
    ('first', 'Returns the first value of column for a group of rows', FALSE, 'default'),
    ('first_value', 'Returns the first value of column for a group of rows', FALSE, 'default'),
    ('kurtosis', 'Returns the kurtosis value calculated from values of a group', TRUE, 'double'),
    ('last', 'Returns the last value of column for a group of rows', FALSE, 'default'),
    ('last_value', 'Returns the last value of column for a group of rows', FALSE, 'default'),
    ('max', 'Returns the maximum value of column', FALSE, 'default'),
    ('max_by', 'Returns the value of `x` associated with the maximum value of `y`.', FALSE, 'default'),
    ('mean', 'Returns the mean calculated from values of a group', TRUE, 'double'),
    ('min', 'Returns the minimum value of column', FALSE, 'default'),
    ('min_by', 'Returns the value of `x` associated with the minimum value of `y`.', FALSE, 'default'),
('percentile', 'Returns the exact percentile value of numeric column `col` at the given percentage. The value of percentage must be between 0.0 and 1.0. The value of frequency should be positive integral', TRUE, 'decimal'),
('percentile_approx', 'Returns the approximate percentile value of numeric column `col` at the given percentage. The value of percentage must be between 0.0 and 1.0. The `accuracy` parameter (default: 10000) is a positive numeric literal which controls approximation accuracy at the cost of memory. Higher value of `accuracy` yields better accuracy.', TRUE, 'decimal'),
    ('skewness', 'Returns the skewness value calculated from values of a group', TRUE, 'double'),
    ('some', 'Returns true if at least one value of `expr` is true.', FALSE, 'boolean'),
    ('std', 'Returns the sample standard deviation calculated from values of a group', TRUE, 'double'),
    ('stddev', 'Returns the sample standard deviation calculated from values of a group', TRUE, 'double'),
    ('stddev_pop', 'Returns the population standard deviation calculated from values of a group.', TRUE, 'double'),
    ('stddev_samp', 'Returns the sample standard deviation calculated from values of a group.', TRUE, 'double'),
    ('sum', 'Returns the sum calculated from values of a group.', TRUE, 'default'),
    ('var_pop', 'Returns the population variance calculated from values of a group', TRUE, 'double'),
    ('var_samp', 'Returns the sample variance calculated from values of a group.', TRUE, 'double'),
    ('variance', 'Returns the sample variance calculated from values of a group', TRUE, 'double')
;

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

CREATE TABLE IF NOT EXISTS meta.output_source
(
    output_source_id integer NOT NULL DEFAULT nextval('meta.seq_output_source'::regclass),
    source_id integer NOT NULL,
    output_id integer NOT NULL,
    filter text COLLATE pg_catalog."default",
    operation_type text COLLATE pg_catalog."default",
    unpivot_list text[] COLLATE pg_catalog."default",
    union_output_position integer,
    active_flag boolean NOT NULL,
    created_userid text COLLATE pg_catalog."default" NOT NULL DEFAULT "current_user"(),
    create_datetime timestamp without time zone NOT NULL DEFAULT now(),
    updated_userid text COLLATE pg_catalog."default",
    update_datetime timestamp without time zone,
    output_package_parameters jsonb,
    output_source_name text COLLATE pg_catalog."default" NOT NULL,
    include_pass_flag boolean,
    include_fail_flag boolean,
    include_warn_flag boolean,
    description text,
    ipu_weight int,
    CONSTRAINT pk_output_source PRIMARY KEY (output_source_id),
    CONSTRAINT output_source_source_name UNIQUE (output_id, source_id, output_source_name),
    CONSTRAINT fk_output_source_output_id FOREIGN KEY (output_id)
        REFERENCES meta.output (output_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_output_source_source FOREIGN KEY (source_id)
        REFERENCES meta.source (source_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE INDEX IF NOT EXISTS "IX_output_source_output_id"
    ON meta.output_source USING btree
    (output_id)
    TABLESPACE pg_default;




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
    CONSTRAINT raw_attribute_raw_attribute_name_data_type UNIQUE(source_id, raw_attribute_name, data_type, datatype_schema),
    CONSTRAINT raw_attribute_column_alias UNIQUE(column_alias, source_id)
);

CREATE INDEX IF NOT EXISTS ix_raw_attribute_source_id  ON meta.raw_attribute (source_id);CREATE TABLE IF NOT EXISTS meta.enrichment_aggregation (
     enrichment_aggregation_id int NOT NULL,
     enrichment_id int NOT NULL,
     expression text,
     function text,
     relation_ids int[],
     create_datetime timestamp without time zone NOT NULL DEFAULT now(),
     CONSTRAINT pk_enrichment_aggregation PRIMARY KEY (enrichment_id, enrichment_aggregation_id),
     CONSTRAINT enrichment_aggregation_enrichment_id_fkey FOREIGN KEY (enrichment_id) 
     REFERENCES meta.enrichment(enrichment_id) ON DELETE CASCADE
 );


CREATE INDEX IF NOT EXISTS ix_enrichment_aggregation_enrichment_id
    ON meta.enrichment_aggregation (enrichment_id);  

  CREATE TABLE IF NOT EXISTS meta.enrichment_parameter (
     enrichment_parameter_id int NOT NULL,
     aggregation_id int NULL,
     parent_enrichment_id int, -- container for the parameter
     type text NOT NULL, -- raw, enrichment, system
     enrichment_id int NULL, -- enrichment_id
     raw_attribute_id int NULL, --  raw_metadata_id
     system_attribute_id int NULL, -- system_attribute_id
     source_id int , -- redundant, can be inferred from enrichment / raw/ system tables
     source_relation_ids int[],
     self_relation_container text,
     create_datetime timestamp without time zone NOT NULL DEFAULT now(),
     CONSTRAINT pk_enrichment_parameter PRIMARY KEY (parent_enrichment_id, enrichment_parameter_id),
     CONSTRAINT enrichment_parameter_parent_enrichment_id FOREIGN KEY (parent_enrichment_id) REFERENCES meta.enrichment(enrichment_id) ON DELETE CASCADE,
     CONSTRAINT enrichment_parameter_enrichment_id FOREIGN KEY (enrichment_id) REFERENCES meta.enrichment(enrichment_id) ON DELETE CASCADE,
     CONSTRAINT enrichment_parameter_raw_attribute_id FOREIGN KEY (raw_attribute_id) REFERENCES meta.raw_attribute(raw_attribute_id),
     CONSTRAINT enrichment_parameter_system_attribute_id FOREIGN KEY (system_attribute_id) REFERENCES  meta.system_attribute(system_attribute_id),
     CONSTRAINT enrichment_parameter_source_id FOREIGN KEY (source_id) REFERENCES meta.source(source_id),
     CONSTRAINT enrichment_parameter_not_blank CHECK(enrichment_id IS NOT NULL OR raw_attribute_id IS NOT NULL OR system_attribute_id IS NOT NULL),
     CONSTRAINT enrichment_parameter_type_check CHECK(type IN ('raw','enrichment','system'))
     );



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
  CREATE TABLE IF NOT EXISTS meta.source_relation_parameter (
     source_relation_parameter_id int NOT NULL,
     source_relation_id int, -- container for the parameter
     type text NOT NULL CHECK(type IN ('raw','enrichment','system')), -- raw, enriched, system
     enrichment_id int NULL, -- enrichment_id
     raw_attribute_id int NULL, --  raw_metadata_id
     system_attribute_id int NULL, -- system_attribute_id
     source_id int , -- redundant, but very handy. can be inferred from enrichment / raw/ system tables
     self_relation_container text,
     CONSTRAINT pk_source_relation_parameter_id PRIMARY KEY (source_relation_id, source_relation_parameter_id),
     CONSTRAINT FK_source_relation_parameter_source_relation FOREIGN KEY (source_relation_id) REFERENCES meta.source_relation(source_relation_id)  ON DELETE CASCADE,
     CONSTRAINT FK_source_relation_parameter_enrichment_id FOREIGN KEY (enrichment_id) REFERENCES meta.enrichment(enrichment_id) ON DELETE CASCADE,
     CONSTRAINT FK_source_relation_parameter_raw_attribute_id FOREIGN KEY (raw_attribute_id) REFERENCES meta.raw_attribute(raw_attribute_id),
     CONSTRAINT FK_source_relation_parameter_attribute_id FOREIGN KEY (system_attribute_id) REFERENCES  meta.system_attribute(system_attribute_id),
     CONSTRAINT FK_source_relation_parameter__source_id FOREIGN KEY (source_id) REFERENCES meta.source(source_id),
     CONSTRAINT relation_parameter_not_blank CHECK(enrichment_id IS NOT NULL OR raw_attribute_id IS NOT NULL OR system_attribute_id IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS log.actor_log
(
    log_id integer NOT NULL,
    message text COLLATE pg_catalog."default" NOT NULL,
    actor_path text COLLATE pg_catalog."default" NOT NULL,
    severity character(1) COLLATE pg_catalog."default" NOT NULL,
    insert_datetime timestamp without time zone NOT NULL,
    db_insert_datetime timestamp without time zone NOT NULL DEFAULT now(),
    job_run_id int,
    CONSTRAINT actor_log_severity_check CHECK (severity = ANY (ARRAY['D'::bpchar, 'W'::bpchar, 'I'::bpchar, 'E'::bpchar]))
);

CREATE INDEX IF NOT EXISTS "IX_actor_log_log_id"
    ON log.actor_log USING btree
    (log_id)
    TABLESPACE pg_default;

	
CREATE INDEX IF NOT EXISTS "IX_actor_log_job_run_id"
    ON log.actor_log USING btree
    (job_run_id)
    TABLESPACE pg_default;
	

CREATE TABLE IF NOT EXISTS meta.import
(
    import_id integer NOT NULL GENERATED ALWAYS AS IDENTITY,
    project_id int NOT NULL,
    type text NOT NULL DEFAULT 'import', 
    status_code char(1),
    log_id int NOT NULL DEFAULT nextval('log.seq_log'::regclass),
    parameters jsonb,
    file_name text,
    format text,
    format_spec text,
    create_datetime timestamp without time zone NOT NULL DEFAULT now(),
    created_userid text COLLATE pg_catalog."default",
    CONSTRAINT pk_import PRIMARY KEY (import_id),
    CONSTRAINT fk_import_project FOREIGN KEY (project_id) REFERENCES meta.project (project_id)
);


CREATE TABLE IF NOT EXISTS meta.import_object
(
    import_object_id integer NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    import_id integer NOT NULL,
    file_path text, -- sources, outputs, ..
    object_type text, -- source/output/group/...
    name text, -- object name
    hash text, -- md5 hash of file
    id int, -- object_id, e.g. source_id
    body_text text, -- text of imported json file
    body jsonb, -- parsed jsonb json file,
    changed_flag boolean NOT NULL DEFAULT false, -- True if object has changed vs. existing
    new_flag boolean, -- true if object was added by import
    rules_changed_flag boolean,
    channels_changed_flag boolean,
    raw_attributes_changed_flag boolean,
    tokens_changed_flag boolean,
    relations_changed_flag boolean,
    dependencies_changed_flag boolean,
    output_columns_changed_flag boolean,
    CONSTRAINT ux_import_object_name UNIQUE (import_id,object_type,name),
    CONSTRAINT ux_import_object_id UNIQUE (import_id,object_type,id),
    CONSTRAINT fk_import_object_import FOREIGN KEY (import_id) REFERENCES meta.import (import_id) ON DELETE CASCADE
);



CREATE TABLE IF NOT EXISTS meta.enrichment_test
(
    enrichment_id INT PRIMARY KEY, 
    project_id int,
    expression text, 
    result json
);CREATE TABLE IF NOT EXISTS meta.output_filter_test
(
    output_source_id INT PRIMARY KEY, 
    project_id int,
    expression text, 
    result json
);CREATE TABLE IF NOT EXISTS meta.relation_test
(
    source_relation_id INT PRIMARY KEY, 
    project_id int,
    expression text, 
    result json
);
CREATE OR REPLACE FUNCTION meta.prc_o_generate_query(in_output_id INT)
    RETURNS TEXT
    LANGUAGE plpgsql
AS
$function$

DECLARE
    v_select_statement TEXT;
    v_from_statement   TEXT;
    v_where_clause     TEXT;
    v_origin_source_id INT;
    v_query            TEXT;
    v_queries           text[] = '{}';
    v_output_id        INT;
    v_output_parameters jsonb;
    v_include_pass_flag boolean;
    v_include_warn_flag boolean;
    v_include_fail_flag boolean;
    v_operation_type text;
    v_refresh_type text;
    v_input_filter text;
    v_output_source_id int;
    v_input_ids int[];
    v_system_fields text;
    v_source_id int;
    v_keep_current_flag boolean;
    v_aggregate_flag boolean;
    v_group_by text;
    v_output_type text;
    v_filter text;
    v_full_output_flag boolean;
    v_cte_structure text;
    v_output_subtype text;
    v_error text;
    v_output_source_ids int[];

BEGIN

SELECT o.output_type, o.output_id, o.output_sub_type
INTO v_output_type, v_output_id, v_output_subtype
FROM meta.output o 
WHERE o.output_id = in_output_id;

SELECT array_agg(os.output_source_id) INTO v_output_source_ids
FROM meta.output_source os WHERE os.output_id = in_output_id;

--Check for fields where the raw_attribute_id, enrichment_id or system_attribute_id has no corresponding record in the DB
with missing_fields AS (
    SELECT CASE WHEN osc.raw_attribute_id IS NOT NULL
                    THEN 'Raw Attribute'
                WHEN osc.enrichment_id IS NOT NULL
                    THEN 'Enrichment'
                WHEN osc.system_attribute_id IS NOT NULL
                    THEN 'System Attribute'
                ELSE 'No Attribute Mapped' END as field_type
           , COALESCE(osc.raw_attribute_id, COALESCE(osc.enrichment_id, osc.raw_attribute_id),0) as field_id
           , oc.name as column_name
           , osc.expression
    FROM meta.output_source_column osc
            JOIN meta.output_column oc ON osc.output_column_id = oc.output_column_id
             LEFT JOIN meta.raw_attribute ra ON osc.raw_attribute_id = ra.raw_attribute_id
             LEFT JOIN meta.enrichment e ON osc.enrichment_id = e.enrichment_id
             LEFT JOIN meta.system_attribute sa ON osc.system_attribute_id = sa.system_attribute_id
    WHERE osc.output_source_id = ANY(v_output_source_ids) AND ra.raw_attribute_id IS NULL AND e.enrichment_id IS NULL AND sa.system_attribute_id IS NULL
)

SELECT 'Columns mapped to missing fields detected: ' || array_agg(field_type || ' with ID ' || field_id || ' mapped into column ' || column_name || ' with expression ' || expression )::text || ' resave or redo these mappings to fix.'
INTO v_error
    FROM missing_fields;

IF v_error IS NOT NULL THEN
    RAISE EXCEPTION '%', v_error;
END IF;


IF v_output_type = 'virtual' THEN
    SELECT array_agg(os.output_source_id) INTO v_output_source_ids
    FROM meta.output_source os
    WHERE os.output_id = v_output_id;
END IF;

    FOREACH v_output_source_id IN ARRAY v_output_source_ids LOOP
        v_group_by := null;

    SELECT os.source_id, o.output_package_parameters || os.output_package_parameters, os.include_pass_flag, os.include_warn_flag, os.include_fail_flag, COALESCE(os.operation_type,'None'), s.refresh_type, s.source_id, os.filter, operation_type = 'Aggregate', COALESCE((o.output_package_parameters->>'full_output_flag')::boolean,false)
    INTO v_origin_source_id, v_output_parameters, v_include_pass_flag, v_include_warn_flag, v_include_fail_flag, v_operation_type, v_refresh_type, v_source_id, v_filter, v_aggregate_flag, v_full_output_flag
    FROM meta.output_source os
    JOIN meta.output o ON os.output_id = o.output_id
    JOIN meta.source s ON os.source_id = s.source_id
    WHERE output_source_id = v_output_source_id;

    v_keep_current_flag := EXISTS(SELECT 1 FROM meta.enrichment WHERE source_id = v_source_id AND keep_current_flag and active_flag);

    PERFORM meta.u_assert(v_include_pass_flag OR v_include_warn_flag OR v_include_fail_flag,'Must include at least one of Pass/Warn/Fail!');
    PERFORM meta.u_assert(v_operation_type<> 'Unpivot', 'Unpivot not supported yet!');


    v_system_fields :=  '';

    v_cte_structure :=  CASE WHEN v_aggregate_flag  AND v_output_type = 'table' THEN ' WITH agg_cte AS( ' ELSE '' END;
    SELECT string_agg(meta.u_output_query_column_select(osc, null) || ' as ' || oc.name
            , ', ' ORDER BY oc.position)
           || ' ' || CASE WHEN v_output_type <> 'table' THEN '' ELSE v_system_fields END
    INTO v_select_statement
    FROM meta.output_column oc
        JOIN meta.output o ON oc.output_id = o.output_id
             LEFT JOIN meta.output_source_column osc ON oc.output_column_id = osc.output_column_id AND osc.output_source_id = v_output_source_id
             LEFT JOIN meta.raw_attribute ra ON osc.raw_attribute_id = ra.raw_attribute_id
             LEFT JOIN meta.enrichment e ON osc.enrichment_id = e.enrichment_id
             LEFT JOIN meta.system_attribute sa ON osc.system_attribute_id = sa.system_attribute_id
    WHERE oc.output_id = v_output_id;


    v_from_statement := ' FROM ' || meta.u_get_hub_table_name(v_origin_source_id, (v_output_parameters->>'key_history')::boolean) || ' T';
    IF v_aggregate_flag THEN

            v_group_by  := ' GROUP BY ' || (SELECT string_agg(CASE WHEN osc.source_relation_ids IS NOT NULL THEN 'J_' || array_to_string(osc.source_relation_ids, '_') ELSE 'T' END || '.' || CASE osc.type WHEN 'raw' THEN ra.column_alias WHEN 'enrichment' THEN e.attribute_name WHEN 'system' THEN sa.name END,', ')
            FROM meta.output_source_column osc LEFT JOIN meta.raw_attribute ra ON osc.raw_attribute_id = ra.raw_attribute_id
             LEFT JOIN meta.enrichment e ON osc.enrichment_id = e.enrichment_id
             LEFT JOIN meta.system_attribute sa ON osc.system_attribute_id = sa.system_attribute_id
                WHERE osc.output_source_id = v_output_source_id AND osc.aggregate IS NULL)
                               --End the aggregate CTE
                               || CASE WHEN v_aggregate_flag  AND v_output_type = 'table' THEN ') SELECT * FROM agg_cte' ELSE '' END;

            v_input_filter := '';

    ELSEIF v_refresh_type = 'full' THEN
         v_input_filter := '';
    ELSE  v_input_filter := '';

     END IF;



    v_where_clause := ' WHERE true ' || COALESCE(' AND ' || REPLACE(NULLIF(v_filter,''),'[This]','T'),'');

    RAISE DEBUG 'FROM: %', v_from_statement;
    RAISE DEBUG 'WHERE: %', v_where_clause;
    RAISE DEBUG 'INPUT: %', v_input_filter; 

    v_queries := v_queries || (v_cte_structure || 'SELECT ' || v_select_statement || v_from_statement || v_where_clause || CASE WHEN v_keep_current_flag OR v_output_type = 'virtual' OR v_full_output_flag THEN '' ELSE  v_input_filter END || COALESCE(v_group_by,''));

    END LOOP;

    v_query := array_to_string(v_queries, ' UNION ALL ');
    RETURN v_query;

EXCEPTION WHEN OTHERS THEN
    RETURN 'QUERY GENERATION ERROR: ' || SQLERRM;

END;


$function$;
CREATE OR REPLACE FUNCTION meta.svcc_get_output_queries(in_import_id int)
 RETURNS json 
 LANGUAGE plpgsql
AS $function$

DECLARE
    v_imp meta.import;
    v_ret json;
BEGIN
    SELECT * INTO v_imp FROM meta.import WHERE import_id = in_import_id;

	-- clean file names
	WITH rgx AS (
        SELECT o.output_id, lower(regexp_replace(left(o.output_name,245),'["<>:\/\\|?*]','_','g')) name_reg
        FROM meta.output o WHERE o.project_id = v_imp.project_id
    ),
	 rwn AS (
		SELECT r.output_id, r.name_reg, ROW_NUMBER() OVER (PARTITION BY r.name_reg ORDER BY r.output_id) rn
	    FROM rgx r
	)
	SELECT json_agg(json_build_object('file_name', CASE WHEN w.rn > 1 THEN w.name_reg || '_' || w.rn ELSE w.name_reg END || '.sql', 
	 'query', meta.prc_o_generate_query(w.output_id) ))
	INTO v_ret
	FROM rwn w;


	RETURN v_ret;


END;

$function$;CREATE OR REPLACE FUNCTION meta.svcc_get_source_queries(in_import_id int)
 RETURNS json 
 LANGUAGE plpgsql
AS $function$

DECLARE
    v_imp meta.import;
    v_ret json;
BEGIN
    SELECT * INTO v_imp FROM meta.import WHERE import_id = in_import_id;

	-- clean file names
	WITH rgx AS (
        SELECT s.source_id, lower(regexp_replace(left(s.source_name,245),'["<>:\/\\|?*]','_','g')) name_reg
        FROM meta.source s WHERE s.project_id = v_imp.project_id
    ),
	 rwn AS (
		SELECT r.source_id, r.name_reg, ROW_NUMBER() OVER (PARTITION BY r.name_reg ORDER BY r.source_id) rn
	    FROM rgx r
	)
	SELECT json_agg(json_build_object('file_name', CASE WHEN w.rn > 1 THEN w.name_reg || '_' || w.rn ELSE w.name_reg END || '.sql', 
	 'query', meta.u_enr_query_generate_query(w.source_id) ))
	INTO v_ret
	FROM rwn w;


	RETURN v_ret;


END;

$function$;CREATE OR REPLACE FUNCTION meta.u_enr_query_add_enrichment(in_enr meta.enrichment)
 RETURNS int -- returns meta.query_element.id
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
v_element_id int;
v_parent_element_ids int[] := ARRAY[]::int[];
v_agg meta.enrichment_aggregation;
v_parameter meta.enrichment_parameter;
v_expression text;
v_ret_element_id int;

BEGIN
SELECT e.id INTO v_element_id
FROM elements e
WHERE e.type = 'enrichment' AND e.attribute_id = in_enr.enrichment_id;

IF v_element_id IS NOT NULL THEN
    -- enrichment already has been added - return element id
    RETURN v_element_id;
END IF;

RAISE DEBUG 'Adding enrichment %', to_json(in_enr);
v_expression := in_enr.expression_parsed;
PERFORM meta.u_assert( v_expression IS NOT NULL, 'expression_parsed is NULL for enrichment=' || to_json(in_enr));
-- Process aggregate parameters
-- a.expression := AVG(P_<parameter_id1> + P_<parameter_id2> + ... 
-- + A_<aggregation_id1> + A_<aggregation_id2> ...
FOR v_agg IN 
    SELECT * 
    FROM meta.enrichment_aggregation a
    WHERE a.enrichment_id = in_enr.enrichment_id
    LOOP
        -- Attribute from another source: add parent TRANSIT element
        v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_transit(v_agg); 
        -- update expression
        -- this assumes that transit element will be on earlier CTE level
        -- if enrichment and transit are going to be on the same level, replace T.TA_... expression with transit element expression
        v_expression := replace(v_expression,'A<' || v_agg.enrichment_aggregation_id || '>', 
        'T.TA_' || v_agg.enrichment_id || '_' || v_agg.enrichment_aggregation_id );

    END LOOP;
-- Process enrichment_parameters 
FOR v_parameter IN 
    SELECT *
    FROM meta.enrichment_parameter p
    WHERE p.parent_enrichment_id  = in_enr.enrichment_id AND p.aggregation_id IS NULL
    LOOP
        IF  v_parameter.source_id = in_enr.source_id AND v_parameter.self_relation_container IS NULL  THEN
                    v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_this_source_element(v_parameter);

            -- update expression
            v_expression := replace(v_expression,'P<' || v_parameter.enrichment_parameter_id || '>', 
            'T.' || meta.u_enr_query_get_enrichment_parameter_name(v_parameter) );
        ELSEIF (v_parameter.source_id <> in_enr.source_id OR v_parameter.self_relation_container IS NOT NULL) THEN
            -- Attribute from another source: add parent TRANSIT element
            v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_transit(v_parameter); 
            -- update expression
            -- this assumes that transit element will be on earlier CTE level
            -- if enrichment and transit are going to be on the same level, replace T.TP_... expression with transit element expression
            v_expression := replace(v_expression,'P<' || v_parameter.enrichment_parameter_id || '>', 
            'T.TP_' || v_parameter.parent_enrichment_id || '_' || v_parameter.enrichment_parameter_id  );
        END IF;
    END LOOP;

-- We now have now added all parents of enrichment
-- Inserting the enrichment record

    WITH cte AS (
    INSERT INTO elements ( type, expression, alias, attribute_id, parent_ids, data_type)
    VALUES ( 'enrichment', v_expression, in_enr.attribute_name, in_enr.enrichment_id, v_parent_element_ids,
            --Check for explicit casts or numeric/decimal types that need to be cast to 38,12
    (SELECT CASE WHEN COALESCE(NULLIF(in_enr.cast_datatype,''), in_enr.datatype) IN ('decimal', 'numeric') OR (COALESCE(in_enr.cast_datatype,'') <> '' AND in_enr.cast_datatype <> in_enr.datatype) THEN at.hive_ddl_type END FROM meta.attribute_type at WHERE at.hive_type = COALESCE(NULLIF(in_enr.cast_datatype,''), in_enr.datatype) ))
    RETURNING id )
    SELECT id INTO v_ret_element_id
    FROM cte;

RETURN v_ret_element_id;        
END;

$function$;CREATE OR REPLACE FUNCTION meta.u_enr_query_add_join(
    in_source_id int, -- this is source we are building join to so we can use it's attribute
    in_source_relation_ids int[])
 RETURNS int -- returns meta.query_element.id
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
v_element_id int;
v_parent_element_ids int[] := ARRAY[]::int[];
v_parent_relation_ids int[];
v_parameter meta.source_relation_parameter;
v_cascading_source_id int;
v_join_alias text;
v_parent_join_alias text;
v_join_expression text;
v_relation_source_id int;
v_attribute_name text;
v_attribute_alias text;
v_ret_element_id int;
v_source_relation_id int := in_source_relation_ids[array_upper(in_source_relation_ids, 1)];
v_cardinality text;
v_uv_enrichment_id int;
v_self_join_flag boolean;
BEGIN
RAISE DEBUG 'Adding join to source_id % for source_relation_ids %', in_source_id, in_source_relation_ids;
PERFORM meta.u_assert( v_source_relation_id IS NOT NULL, 'source_relation_id is null in relation chain=' || in_source_relation_ids::text);

-- chek if join already exists
SELECT e.id INTO v_element_id
FROM elements e
WHERE e.type = 'join' AND e.relation_ids = in_source_relation_ids;

IF v_element_id IS NOT NULL THEN
    -- join already has been added - return element id
    RETURN v_element_id;
END IF;

-- Get relation expression
SELECT sr.expression_parsed, sr.source_id, CASE 
WHEN sr.source_id = sr.related_source_id THEN least(source_cardinality, related_source_cardinality)
WHEN sr.source_id = in_source_id THEN source_cardinality 
WHEN sr.related_source_id = in_source_id THEN related_source_cardinality END
INTO v_join_expression, v_relation_source_id, v_cardinality
FROM meta.source_relation sr 
WHERE sr.source_relation_id = v_source_relation_id ;

PERFORM meta.u_assert( v_join_expression IS NOT NULL, 'expression_parsed is NULL for source_relation_id=' || v_source_relation_id);
PERFORM meta.u_assert( v_cardinality IS NOT NULL, 'Relation source_relation_id=' || v_source_relation_id || ' is not attached to source_id=' || in_source_id);
PERFORM meta.u_assert( v_cardinality = '1', 'Join cardinality is not 1 for source_relation_id=' || v_source_relation_id || ' source_id=' || in_source_id);

-- Process relation parameters: we only need to add parent links to [This] source parameters on the first Join in the chain
-- All cascading joins will not join to [This] and thus will have relation parameters ready
-- relation.expression_parsed := [This].P_<relation_arameter_id1> =  [Related].P_<relation_parameter_id2> + ... 
IF cardinality(in_source_relation_ids) = 1 THEN
FOR v_parameter IN 
    SELECT *
    FROM meta.source_relation_parameter p
    WHERE p.source_relation_id = in_source_relation_ids[1]
        AND --p.source_id <> in_source_id -- exclude relation attributes of the target source as they are already available
        ( (p.source_id <> in_source_id AND p.self_relation_container IS NULL)
            OR (p.source_id = in_source_id AND p.self_relation_container IS NOT NULL)
        )
    LOOP
        PERFORM meta.u_assert( v_parameter.type IS NOT NULL, 'type is NULL for source_relation_parameter_id=' || v_parameter.source_relation_parameter_id);


            v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_this_source_element(v_parameter);

    END LOOP;
    v_parent_join_alias := 'T';
ELSE 
    -- This is cascading join - need to add join parent
    SELECT CASE WHEN in_source_id = sr.source_id THEN sr.related_source_id ELSE sr.source_id END
    INTO v_cascading_source_id
    FROM meta.source_relation sr
    WHERE sr.source_relation_id = v_source_relation_id;

    v_parent_relation_ids := meta.u_remove_last_array_element(in_source_relation_ids);
    v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_join(v_cascading_source_id, v_parent_relation_ids);
    v_parent_join_alias := 'J_' || meta.u_enr_query_relation_alias(v_parent_relation_ids);
END IF;

v_join_alias := 'J_' || meta.u_enr_query_relation_alias(in_source_relation_ids);

-- Find and replace parameters
FOR v_parameter IN 
    SELECT *
    FROM meta.source_relation_parameter p
    WHERE p.source_relation_id = v_source_relation_id 
    LOOP
        v_attribute_name := meta.u_enr_query_get_relation_parameter_name(v_parameter);
        v_attribute_alias := CASE WHEN v_parameter.source_id <> in_source_id THEN v_parent_join_alias
            WHEN v_parameter.source_id = in_source_id AND (v_parameter.self_relation_container IS NULL OR v_parameter.self_relation_container = 'Related') THEN v_join_alias 
            ELSE v_parent_join_alias END;
        v_join_expression := replace(v_join_expression,'P<' || v_parameter.source_relation_parameter_id || '>', v_attribute_alias || '.' || v_attribute_name);
    END LOOP;

-- Add Join unique filters
FOR v_attribute_name, v_uv_enrichment_id, v_self_join_flag IN 
    SELECT e.attribute_name, eu.enrichment_id, eu.source_id = in_source_id AND p.self_relation_container IS NOT NULL
    FROM meta.source_relation_parameter p JOIN meta.enrichment e ON p.enrichment_id = e.enrichment_id
    LEFT JOIN meta.enrichment eu ON eu.parent_enrichment_id = e.enrichment_id AND eu.attribute_name LIKE '%_uv_flag'
    WHERE p.source_relation_id = v_source_relation_id AND p.source_id = in_source_id
    AND p.type= 'enrichment' AND e.unique_flag
    LOOP
        PERFORM meta.u_assert( v_uv_enrichment_id IS NOT NULL, 'Uniqueness validation enrichment is missing or inactive for enrichment ' || v_attribute_name || ' referenced in for source_relation_id=' || v_source_relation_id);
        
        IF v_self_join_flag THEN -- add uniqueness enrichment for self-relation, forcing it to recalculate
            v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_this_source_element('enrichment', v_uv_enrichment_id);
        END IF;

        v_join_expression := v_join_expression || ' AND ' || v_join_alias || '.' 
         || v_attribute_name || '_uv_flag';
    END LOOP;    

-- Inserting the join record

    WITH cte AS (
    INSERT INTO elements ( type, source_id, expression, alias, attribute_id, parent_ids, relation_ids)
    VALUES ( 'join', in_source_id, v_join_expression, v_join_alias, null, v_parent_element_ids, in_source_relation_ids )
    RETURNING id )
    SELECT id INTO v_ret_element_id
    FROM cte;

    RETURN v_ret_element_id;
END;

$function$;CREATE OR REPLACE FUNCTION meta.u_enr_query_add_many_join(in_many_join_source_id int, 
in_source_relation_ids int[])
 RETURNS int -- returns meta.query_element.id
 LANGUAGE plpgsql
 COST 10
AS $function$
DECLARE
v_element_id int;
v_parent_element_ids int[] := '{}';
v_parameter meta.source_relation_parameter;
v_cascading_source_id int;
v_join_alias text;
v_join_expression text;
v_ret_element_id int;
v_source_relation_id int := in_source_relation_ids[array_upper(in_source_relation_ids, 1)];
v_cascading_relation_ids int[] := in_source_relation_ids;
v_many_join_list text[] := '{}';
v_attribute_alias text;

BEGIN
RAISE DEBUG 'Adding many-join to source_id % for source_relation_ids %', in_many_join_source_id, in_source_relation_ids;
PERFORM meta.u_assert( v_source_relation_id IS NOT NULL, 'source_relation_id is null in relation chain=' || in_source_relation_ids::text);

-- chek if many-join already exists
SELECT e.id INTO v_element_id
FROM elements e
WHERE e.type = 'many-join' AND e.relation_ids = in_source_relation_ids;

IF v_element_id IS NOT NULL THEN
    -- sq already has been added - return element id
    RETURN v_element_id;
END IF;

-- Get relation expression
SELECT sr.expression_parsed
INTO v_join_expression
FROM meta.source_relation sr 
WHERE sr.source_relation_id = v_source_relation_id;

PERFORM meta.u_assert( v_join_expression IS NOT NULL, 'expression_parsed is NULL for source_relation_id=' || v_source_relation_id);

-- Process relation parameters: we only need to add parent links to [This] source parameters on the first Join in the chain
-- All cascading joins will not join to [This] and thus will have relation parameters ready
-- relation.expression_parsed := [This].P_<relation_arameter_id1> =  [Related].P_<relation_parameter_id2> + ... 
FOR v_parameter IN 
    SELECT *
    FROM meta.source_relation_parameter p
    WHERE p.source_relation_id = v_source_relation_id
    AND ( (p.source_id <> in_many_join_source_id AND p.self_relation_container IS NULL)
        OR (p.source_id = in_many_join_source_id AND p.self_relation_container IS NOT NULL)
    )
    -- exclude relation attributes of the many-join source as they are already available
    LOOP
        IF cardinality(in_source_relation_ids) = 1
            THEN
                    v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_this_source_element(v_parameter);
        END IF;
        IF  v_parameter.source_id <> in_many_join_source_id OR v_parameter.self_relation_container = 'This' THEN
            v_attribute_alias := CASE WHEN cardinality(in_source_relation_ids) = 1 THEN  meta.u_enr_query_get_relation_parameter_name(v_parameter) 
            ELSE 'TR_' || v_parameter.source_relation_id || '_' || v_parameter.source_relation_parameter_id END;
            v_join_expression := replace(v_join_expression,'P<' || v_parameter.source_relation_parameter_id || '>', 
            'D.' || v_attribute_alias);
            v_many_join_list := v_many_join_list || v_attribute_alias;
        END IF;
    END LOOP;

IF cardinality(in_source_relation_ids) > 1 THEN
     -- This is cascading many-join - need to add transits to parent join
    v_cascading_relation_ids[array_upper(v_cascading_relation_ids, 1)] := null;
    v_cascading_relation_ids = array_remove(v_cascading_relation_ids,null);
    SELECT CASE WHEN in_many_join_source_id = sr.source_id THEN sr.related_source_id ELSE sr.source_id END
    INTO v_cascading_source_id
    FROM meta.source_relation sr
    WHERE sr.source_relation_id = v_source_relation_id;

    FOR v_parameter IN 
        SELECT *
        FROM meta.source_relation_parameter p
        WHERE p.source_relation_id = v_source_relation_id
            AND p.source_id = v_cascading_source_id -- get all relation attributes of the cascading join source
        LOOP
            v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_transit(v_parameter,v_cascading_relation_ids);
            v_join_expression := replace(v_join_expression,'P<' || v_parameter.source_relation_parameter_id || '>', 
            'D.TR_' || v_parameter.source_relation_id || '_' || v_parameter.source_relation_parameter_id /* transit attribute alias*/);
        END LOOP;
END IF;

v_join_alias := 'J_' || meta.u_enr_query_relation_alias(in_source_relation_ids);

-- Find and replace expression parameters on the related side 
FOR v_parameter IN 
    SELECT *
    FROM meta.source_relation_parameter p
    WHERE p.source_relation_id = v_source_relation_id
    AND p.source_id = in_many_join_source_id AND (p.self_relation_container IS NULL OR 
        p.self_relation_container = 'Related')
    LOOP
        v_join_expression := replace(v_join_expression,'P<' || v_parameter.source_relation_parameter_id || '>', 
        'R.' || meta.u_enr_query_get_relation_parameter_name(v_parameter));
    END LOOP;

-- Inserting the many-join record
    WITH cte AS (
    INSERT INTO elements (type, source_id, expression, alias, attribute_id, parent_ids, relation_ids, many_join_list)
    VALUES ( 'many-join', in_many_join_source_id, v_join_expression, v_join_alias, null, v_parent_element_ids, in_source_relation_ids, v_many_join_list )
    RETURNING id )
    SELECT id INTO v_ret_element_id
    FROM cte;

RETURN v_ret_element_id;  
END;

$function$;CREATE OR REPLACE FUNCTION meta.u_enr_query_add_many_join_attribute(in_agg meta.enrichment_aggregation)
 RETURNS int -- returns meta.query_element.id
 LANGUAGE plpgsql
 COST 10
AS $function$
DECLARE
v_element_id int;
v_parent_element_ids int[] := '{}';
v_attribute_name text;
v_attribute_alias text;
v_expression text := in_agg.expression;
v_parameter meta.enrichment_parameter;
v_source_id int; -- [This] source_id of the parent enrichment
v_many_join_source_ids int[];
v_ret_element_id int;
v_alias text := 'A_' || in_agg.enrichment_id || '_' || in_agg.enrichment_aggregation_id;

BEGIN
-- chek if attribute already exists
SELECT e.id INTO v_element_id
FROM elements e
WHERE e.type = 'many-join attribute' 
AND e.attribute_id = in_agg.enrichment_aggregation_id AND e.alias = v_alias;

IF v_element_id IS NOT NULL THEN
    -- attribute already has been added - return element id
    RETURN v_element_id;
END IF;

RAISE DEBUG 'Adding many-join attribute for enrichment_aggregation %', to_json(in_agg);
PERFORM meta.u_assert(in_agg.relation_ids IS NOT NULL, 'relation_ids is NULL for enrichment_aggregation ' || to_json(in_agg));

SELECT e.source_id INTO v_source_id FROM meta.enrichment e
WHERE e.enrichment_id = in_agg.enrichment_id;

v_many_join_source_ids := meta.u_enr_query_get_related_source_ids(v_source_id,in_agg.relation_ids);

v_parent_element_ids := v_parent_element_ids || 
    meta.u_enr_query_add_many_join(v_many_join_source_ids[array_upper(v_many_join_source_ids,1)],
    in_agg.relation_ids);

-- process parameters of aggregation
FOR v_parameter IN 
    SELECT *
    FROM meta.enrichment_parameter p
    WHERE in_agg.enrichment_aggregation_id = p.aggregation_id AND p.parent_enrichment_id = in_agg.enrichment_id
    LOOP
    -- Get parameter name
        v_attribute_name := meta.u_enr_query_get_enrichment_parameter_name(v_parameter);
        -- attribute can come from :
        IF v_parameter.source_id = v_source_id AND v_parameter.self_relation_container IS NULL  THEN
            v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_this_source_element(v_parameter);


            v_attribute_alias := 'T';
        ELSEIF v_parameter.source_relation_ids = in_agg.relation_ids THEN
            -- attribute from many-join source: we already have many-join as a parent
            v_attribute_alias := 'R';
        ELSE
            -- This parameter is not from [This] or many-join source: add transit attribute
            v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_transit(v_parameter);
            v_attribute_alias := 'T'; 
            v_attribute_name := 'TP_' || v_parameter.parent_enrichment_id || '_' || v_parameter.enrichment_parameter_id;
        END IF;
        
        v_expression := replace(v_expression,'P<' || v_parameter.enrichment_parameter_id || '>',
                v_attribute_alias || '.' || v_attribute_name);
    END LOOP;

-- Inserting the  record
    WITH cte AS (
    INSERT INTO elements ( type, expression, alias, attribute_id, parent_ids, relation_ids)
    VALUES ('many-join attribute', v_expression, v_alias, 
            in_agg.enrichment_aggregation_id, v_parent_element_ids, in_agg.relation_ids )
    RETURNING id )
    SELECT id INTO v_ret_element_id
    FROM cte;

    RETURN v_ret_element_id;
END;

$function$;DROP FUNCTION IF EXISTS meta.u_enr_query_add_sub_query( int,  int[]);
DROP FUNCTION IF EXISTS meta.u_enr_query_add_sub_query_attribute(meta.enrichment_aggregation);CREATE OR REPLACE FUNCTION meta.u_enr_query_add_this_source_element(v_parameter meta.enrichment_parameter)
RETURNS int
 LANGUAGE plpgsql
 COST 10
AS $function$
DECLARE
    v_ret int;
    BEGIN
 -- [This] source:  enrichment_parameter.source_id = enrichment.source_id
            IF v_parameter.type = 'enrichment' THEN
                -- Enrichment parameter from same source: add parent enrichment element
                SELECT meta.u_enr_query_add_enrichment(enr)
                INTO v_ret
                FROM meta.enrichment enr
                WHERE enr.enrichment_id = v_parameter.enrichment_id;

                ELSEIF v_parameter.type = 'raw' THEN
                -- Raw parameter from same source: add parent elements
                SELECT e.id
                INTO v_ret
                FROM elements e
                WHERE e.type = 'raw' AND e.attribute_id = v_parameter.raw_attribute_id;

            ELSEIF v_parameter.type = 'system' THEN
                -- System parameter from same source: add parent elements
                SELECT e.id
                INTO v_ret
                FROM elements e
                WHERE e.type = 'system' AND e.attribute_id = v_parameter.system_attribute_id;

                END IF;
    PERFORM meta.u_assert( v_ret IS NOT NULL, 'unable to lookup [This] source enrichment parameter: ' || to_json(v_parameter)::text);


 RETURN v_ret;

END;
$function$;

CREATE OR REPLACE FUNCTION meta.u_enr_query_add_this_source_element(v_parameter meta.source_relation_parameter)
RETURNS int
 LANGUAGE plpgsql
 COST 10
AS $function$
DECLARE
    v_ret int;
    BEGIN
 -- [This] source:  enrichment_parameter.source_id = enrichment.source_id
            IF v_parameter.type = 'enrichment' THEN
                -- Enrichment parameter from same source: add parent enrichment element
                SELECT meta.u_enr_query_add_enrichment(enr)
                INTO v_ret
                FROM meta.enrichment enr
                WHERE enr.enrichment_id = v_parameter.enrichment_id;

                ELSEIF v_parameter.type = 'raw' THEN
                -- Raw parameter from same source: add parent elements
                SELECT e.id
                INTO v_ret
                FROM elements e
                WHERE e.type = 'raw' AND e.attribute_id = v_parameter.raw_attribute_id;

            ELSEIF v_parameter.type = 'system' THEN
                -- System parameter from same source: add parent elements
                SELECT e.id
                INTO v_ret
                FROM elements e
                WHERE e.type = 'system' AND e.attribute_id = v_parameter.system_attribute_id;

                END IF;

    PERFORM meta.u_assert( v_ret IS NOT NULL, 'unable to lookup [This] source relation parameter %s' || to_json(v_parameter)::text);

 RETURN v_ret;

END;
$function$;

CREATE OR REPLACE FUNCTION meta.u_enr_query_add_this_source_element(in_type text, in_id int)
RETURNS int
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_ret int;
    BEGIN
 -- [This] source:  enrichment_parameter.source_id = enrichment.source_id
            IF in_type = 'enrichment' THEN
                -- Enrichment parameter from same source: add parent enrichment element
                SELECT meta.u_enr_query_add_enrichment(enr)
                INTO v_ret
                FROM meta.enrichment enr
                WHERE enr.enrichment_id = in_id;

            ELSEIF in_type = 'raw' THEN
                -- Raw parameter from same source: add parent elements
                SELECT e.id
                INTO v_ret
                FROM elements e
                WHERE e.type = 'raw' AND e.attribute_id = in_id;

            ELSEIF in_type = 'system' THEN
                -- System parameter from same source: add parent elements
                SELECT e.id
                INTO v_ret
                FROM elements e
                WHERE e.type = 'system' AND e.attribute_id = in_id;

                END IF;

 RETURN v_ret;

END;
$function$;CREATE OR REPLACE FUNCTION meta.u_enr_query_add_transit(in_parameter meta.enrichment_parameter)
 RETURNS int -- returns meta.query_element.id
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
v_element_id int;
v_parent_element_ids int[] := '{}';
v_expression text;
v_join_alias text;
v_transit_alias text;
v_ret_element_id int;

BEGIN
v_transit_alias := 'TP_' || in_parameter.parent_enrichment_id || '_' || in_parameter.enrichment_parameter_id;
SELECT e.id INTO v_element_id
FROM elements e
WHERE e.type = 'transit' AND e.attribute_id = in_parameter.enrichment_parameter_id AND e.alias = v_transit_alias
AND in_parameter.source_relation_ids = e.relation_ids;

IF v_element_id IS NOT NULL THEN
    -- enrichment already has been added - return element id
    RETURN v_element_id;
END IF;
RAISE DEBUG 'Adding transit for enrichment_parameter %', to_json(in_parameter);
PERFORM meta.u_assert( cardinality(in_parameter.source_relation_ids) > 0, 'Relation chain cannot be blank for transit element. enrichment_parameter=' || to_json(in_parameter));
-- Attribute from another source: add parent JOIN element
v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_join(in_parameter.source_id, in_parameter.source_relation_ids); 
v_join_alias := 'J_' || meta.u_enr_query_relation_alias(in_parameter.source_relation_ids);

v_expression := v_join_alias || '.' || meta.u_enr_query_get_enrichment_parameter_name(in_parameter);
-- Inserting the transit record
WITH cte AS (
INSERT INTO elements ( type, expression, alias, attribute_id, parent_ids, relation_ids)
VALUES ( 'transit', v_expression, v_transit_alias, 
in_parameter.enrichment_parameter_id, v_parent_element_ids, in_parameter.source_relation_ids )
RETURNING id )
SELECT id INTO v_ret_element_id
FROM cte;

RETURN v_ret_element_id;  
END;

$function$;

CREATE OR REPLACE FUNCTION meta.u_enr_query_add_transit(in_parameter meta.source_relation_parameter,
in_relation_ids int[])
 RETURNS int -- returns meta.query_element.id
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
v_element_id int;
v_parent_element_ids int[] := '{}';
v_expression text;
v_join_alias text;
v_transit_alias text;
v_ret_element_id int;
BEGIN
v_transit_alias := 'TR_' || in_parameter.source_relation_id || '_' || in_parameter.source_relation_parameter_id;

SELECT e.id INTO v_element_id
FROM elements e
WHERE e.type = 'transit' AND e.alias = v_transit_alias
AND in_relation_ids = e.relation_ids;

IF v_element_id IS NOT NULL THEN
    -- enrichment already has been added - return element id
    RETURN v_element_id;
END IF;

RAISE DEBUG 'Adding transit for relation_parameter %', to_json(in_parameter);
PERFORM meta.u_assert( cardinality(in_relation_ids) > 0, 'Relation chain cannot be blank for transit element. relation_parameter=' || to_json(in_parameter));

-- Attribute from another source: add parent JOIN element
v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_join(in_parameter.source_id, in_relation_ids); 
v_join_alias := 'J_' || meta.u_enr_query_relation_alias(in_relation_ids);

v_expression := v_join_alias || '.' || meta.u_enr_query_get_enrichment_parameter_name(in_parameter);

-- Inserting the transit record
    WITH cte AS (
    INSERT INTO elements ( type, expression, alias, attribute_id, parent_ids, relation_ids)
    VALUES ( 'transit', v_expression, v_transit_alias, 
    in_parameter.source_relation_parameter_id, v_parent_element_ids, in_relation_ids )
    RETURNING id )
    SELECT id INTO v_ret_element_id
    FROM cte;

    RETURN v_ret_element_id;
END;

$function$;

CREATE OR REPLACE FUNCTION meta.u_enr_query_add_transit(in_parameter meta.enrichment_aggregation)
 RETURNS int -- returns meta.query_element.id
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
v_element_id int;
v_parent_element_ids int[] := '{}';
v_expression text;
v_join_alias text;
v_transit_alias text;
v_ret_element_id int;
BEGIN
v_transit_alias := 'TA_' || in_parameter.enrichment_id || '_' || in_parameter.enrichment_aggregation_id;

SELECT e.id INTO v_element_id
FROM elements e
WHERE e.type = 'transit-agg' AND e.attribute_id = in_parameter.enrichment_aggregation_id
AND e.alias = v_transit_alias
AND in_parameter.relation_ids = e.relation_ids;

IF v_element_id IS NOT NULL THEN
    -- enrichment already has been added - return element id
    RETURN v_element_id;
END IF;

RAISE DEBUG 'Adding transit for aggregation_parameter %', to_json(in_parameter);
PERFORM meta.u_assert( cardinality(in_parameter.relation_ids) > 0, 'Relation chain cannot be blank for transit element. aggregation_parameter=' || to_json(in_parameter));

-- Attribute from another source: add parent many-join attribute element

    v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_many_join_attribute(in_parameter);
    
    v_join_alias := 'J_' || meta.u_enr_query_relation_alias(in_parameter.relation_ids) || '_AGG';
    v_expression := v_join_alias || '.A_' || in_parameter.enrichment_id || '_' || in_parameter.enrichment_aggregation_id;

-- Inserting the transit record
    WITH cte AS (
    INSERT INTO elements ( type, expression, alias, attribute_id, parent_ids, relation_ids)
    VALUES ( 'transit-agg', v_expression, v_transit_alias, 
    in_parameter.enrichment_aggregation_id, v_parent_element_ids, in_parameter.relation_ids )
    RETURNING id )
    SELECT id INTO v_ret_element_id
    FROM cte;

    RETURN v_ret_element_id;
END;

$function$; 
CREATE OR REPLACE FUNCTION meta.u_enr_query_add_validation_status(
    in_source_id int)
 RETURNS int -- returns meta.query_element.id
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
    v_element_id int;
    v_val_status_parents int[];
    v_expression text;
    v_expression_fail text;
    v_expression_warn text;
BEGIN
RAISE DEBUG 'Adding s_validation_status_code to %', in_source_id;

-- chek if already exists
SELECT e.id INTO v_element_id
FROM elements e
WHERE e.type = 'enrichment' AND e.attribute_id = -1;

IF v_element_id IS NOT NULL THEN
    -- already has been added - return element id
    RETURN v_element_id;
END IF;

-- Get list of all validation rule elements
SELECT array_agg(el.id)
INTO v_val_status_parents
FROM meta.enrichment e LEFT JOIN elements el
ON e.enrichment_id = el.attribute_id AND el.type = 'enrichment'
WHERE e.source_id = in_source_id AND e.active_flag AND e.rule_type_code = 'V';

IF array_position(v_val_status_parents, NULL) IS NOT NULL THEN
    RAISE DEBUG 'Can''t add s_validation_status_code calculation, not all validation rules have been added yet.';
    RETURN null;
END IF;

-- All validations have been added - we can add status calculation
SELECT string_agg('T.' || el.alias, ' AND ') 
INTO v_expression_fail
FROM meta.enrichment e JOIN elements el
ON e.enrichment_id = el.attribute_id 
WHERE el.id = ANY(v_val_status_parents) AND e.validation_action_code = 'F';

SELECT string_agg('T.' || el.alias, ' AND ') 
INTO v_expression_warn
FROM meta.enrichment e JOIN elements el
ON e.enrichment_id = el.attribute_id 
WHERE el.id = ANY(v_val_status_parents) AND e.validation_action_code = 'W';

IF v_expression_fail IS NULL AND v_expression_warn IS NULL THEN 
    v_expression := 'CAST(''P'' as char(1))';
ELSE
    v_expression := 'CAST(CASE ' || COALESCE('WHEN NOT (' || v_expression_fail || ') THEN ''F'' ', '')
    || COALESCE('WHEN NOT (' || v_expression_warn || ') THEN ''W'' ','') || ' ELSE ''P'' END as char(1))';
END IF; 

WITH ct AS (
    INSERT INTO elements ( type, expression, alias, attribute_id, parent_ids, cte)
    VALUES ( 'enrichment', v_expression , 's_validation_status_code', -1, v_val_status_parents,
    CASE WHEN v_val_status_parents IS NULL THEN 0 END)
    RETURNING id )
    SELECT id INTO v_element_id FROM ct; 

RETURN v_element_id;
END;

$function$;CREATE OR REPLACE FUNCTION meta.u_enr_query_find_in_parents(in_child_id int,
    in_parent_id int)
 RETURNS boolean -- returns true if child element has in_parent in the parental chain
 LANGUAGE plpgsql
 COST 10
AS $function$
BEGIN

RETURN EXISTS(

WITH RECURSIVE ct AS (
    SELECT e.id, e.parent_ids FROM elements e 
	WHERE e.id = in_child_id
  UNION ALL
    SELECT e.id, e.parent_ids 
	FROM elements e JOIN ct ON ct.parent_ids @> ARRAY[e.id]
)
SELECT 1 FROM ct WHERE ct.id = in_parent_id AND ct.id <> in_child_id
);
END;

$function$;
CREATE OR REPLACE FUNCTION meta.u_enr_query_generate_distinct_many_join_query(in_source_id int, in_cte int)
    
 RETURNS text
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
v_sql text := '';
v_el meta.query_element;
v_group_by text;
v_agg_list text;

BEGIN

FOR v_el IN 
    SELECT * 
    FROM elements e
    WHERE e.cte = in_cte AND e.type = 'many-join'
    LOOP
        RAISE DEBUG 'Adding DIST/AGG queries for element %', to_json(v_el);
        PERFORM meta.u_assert( cardinality(v_el.many_join_list) >= 1, 'Many join list cardinality is 0 or null');
        
        v_sql := CASE WHEN v_sql = '' THEN '' ELSE v_sql || E',\n' END;
        -- DISTINCT query
        v_sql := v_sql || v_el.alias || '_DIST AS (SELECT DISTINCT ' || array_to_string(v_el.many_join_list,',') || 
        ' FROM ' || CASE WHEN in_cte = 0 THEN 'input' ELSE 'cte' || (in_cte - 1) END || '),
        ';
        
        -- Aggregate query
        -- list of GROUP BY attributes
        v_group_by := (SELECT string_agg('D.' || DL.att, ',') FROM unnest(v_el.many_join_list) DL(att) );
        PERFORM meta.u_assert( v_group_by IS NOT NULL,'Group by list is NULL');

        v_agg_list := (SELECT string_agg(e.expression || ' ' || e.alias, ',') 
        FROM elements e WHERE e.type = 'many-join attribute' AND e.parent_ids @> ARRAY[v_el.id] );
        PERFORM meta.u_assert( v_agg_list IS NOT NULL,'Aggregate list is NULL');
        
        v_sql := v_sql || v_el.alias || '_AGG AS (SELECT ' || v_group_by || ',' || v_agg_list
        || '
        FROM ' || v_el.alias || '_DIST D JOIN ' || CASE WHEN in_source_id = v_el.source_id AND in_cte > 0 THEN 'cte' || (in_cte - 1) -- self-join
        ELSE meta.u_get_hub_table_name(v_el.source_id) END || ' R ON '
        || v_el.expression || '
        GROUP BY ' || v_group_by || ')
        ';

END LOOP;

IF v_sql != '' AND in_cte > 0 THEN 
    v_sql := ',' || v_sql; 
END IF;

RETURN v_sql;

END;

$function$;
CREATE OR REPLACE FUNCTION meta.u_enr_query_generate_elements(in_source_id int, in_mode text,
in_input_id int, in_enr_ids int[])
 RETURNS SETOF meta.query_element
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
v_refresh_type text = 'full';
v_starting_table_type text;
v_cte int;
v_enr_children int[];
v_val_status_parents int[];
v_raw_attribute_ids int[];
    v_input_id_rules_flag boolean;

BEGIN
PERFORM meta.u_assert( in_mode IN ('input','recalculation','reset'), 'Unknown mode=' || in_mode);
PERFORM meta.u_assert( CASE WHEN in_mode = 'recalculation' THEN in_enr_ids IS NOT NULL 
WHEN in_mode = 'input' THEN in_input_id IS NOT NULL ELSE true END, 'Required parameters not provided');

DROP TABLE IF EXISTS elements;
CREATE TEMP TABLE elements (like meta.query_element INCLUDING ALL) ON COMMIT DROP;



-- Add all row attributes 
    INSERT INTO elements ( type, expression, alias, attribute_id, cte)
    SELECT 'raw', 'T.' || r.column_alias, r.column_alias, r.raw_attribute_id, 0 
    FROM meta.raw_attribute r 
    WHERE r.source_id = in_source_id;	

-- Add all system attributes
INSERT INTO elements ( type, expression, alias, attribute_id, cte)
SELECT  'system', 'T.' || sa.name, sa.name, system_attribute_id, 0  
FROM meta.system_attribute sa;

-- add s_latest_flag for bulk reset CDC and input delete
IF in_mode = 'reset' AND v_refresh_type = 'key' THEN
    INSERT INTO elements ( type, expression, alias, attribute_id, cte)
    VALUES  ('system', 'T.s_latest_flag', 's_latest_flag', 1000, 0 );
END IF;

-- Add all enrichment attributes
IF in_mode IN ('input','reset') THEN
    -- Get all keep_current window functions and their child enrichments
    SELECT COALESCE(array_agg(e.enrichment_id),'{}'::int[]) 
    INTO v_enr_children 
    FROM meta.enrichment e
    WHERE e.source_id = in_source_id AND e.active_flag 
    AND (e.window_function_flag AND e.keep_current_flag);
    IF cardinality(v_enr_children) > 0 THEN -- ADD children of window functions
        v_enr_children := v_enr_children || meta.u_enr_query_get_enrichment_children(in_source_id,v_enr_children);
    END IF;
    -- Add all enrichments that would not require recalculation
    PERFORM  meta.u_enr_query_add_enrichment(e)
    FROM meta.enrichment e
    WHERE e.source_id = in_source_id AND e.active_flag 
    AND e.enrichment_id <> ALL (v_enr_children);
    -- Add validation status
    PERFORM meta.u_enr_query_add_validation_status(in_source_id);

ELSEIF in_mode = 'recalculation' THEN
    v_enr_children := in_enr_ids || meta.u_enr_query_get_enrichment_children(in_source_id,in_enr_ids);

    -- add all pre-calculated enrichments, excluding those that need recalculation and their children
    INSERT INTO elements ( type, expression, alias, attribute_id, cte)
    SELECT 'enrichment', 'T.' || e.attribute_name, e.attribute_name, e.enrichment_id, 0 
    FROM meta.enrichment e
    WHERE e.source_id = in_source_id AND e.active_flag
    AND e.enrichment_id <> ALL (v_enr_children);
    -- add enrichments requiring recalculation
    PERFORM  meta.u_enr_query_add_enrichment(e)
    FROM meta.enrichment e
    WHERE e.source_id = in_source_id AND e.active_flag
    AND e.enrichment_id = ANY (v_enr_children);
    -- Add validation status
    PERFORM meta.u_assert( meta.u_enr_query_add_validation_status(in_source_id) IS NOT NULL, 'Could not add s_validation_status_code');

END IF;

-- assign cte to each element
FOR v_cte IN 0 .. 1000 LOOP
    PERFORM meta.u_enr_query_update_cte(v_cte, in_mode);
    EXIT WHEN NOT EXISTS(SELECT 1 FROM elements e WHERE e.cte IS NULL);
END LOOP;

RETURN QUERY SELECT * FROM elements;

END;

$function$;CREATE OR REPLACE FUNCTION meta.u_enr_query_generate_query(in_source_id int, in_mode text,
in_input_id int, in_enr_ids int[])
    
 RETURNS text
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
v_cte int;
v_sql text;
v_cte_max int;
v_many_join_query text;

BEGIN
PERFORM meta.u_assert( in_mode IN ('input', 'recalculation', 'reset'),'Unknown mode=' || in_mode);


IF in_mode = 'recalculation' AND in_enr_ids IS NULL THEN 
    -- This is recalculation phase triggered by new Input on [This] source: add window function enrichments with keep_current flag  
    SELECT array_agg(enrichment_id)
    INTO in_enr_ids
    FROM meta.enrichment e
    WHERE e.source_id = in_source_id AND e.active_flag 
    AND (e.window_function_flag AND e.keep_current_flag); 
    IF in_enr_ids IS NULL THEN
        RETURN NULL; -- nothing to do
    END IF;
END IF;

PERFORM meta.u_assert( CASE WHEN in_mode = 'input' THEN in_input_id IS NOT NULL ELSE true END,'input_id is NULL');

PERFORM meta.u_enr_query_generate_elements(in_source_id,in_mode, in_input_id, in_enr_ids);
PERFORM meta.u_assert( NOT EXISTS(SELECT 1 FROM elements WHERE cte IS NULL), 'Error calculating element CTE groups. Review output of meta.u_enr_query_generate_elements for details');
SELECT MAX(cte) INTO v_cte_max FROM elements;

v_sql := '/*Compiled on ' || now() || ' mode=' || in_mode || '*/
' || CASE WHEN v_cte_max > 0 OR EXISTS(SELECT 1 FROM elements e WHERE e.type = 'many-join attribute') THEN 'WITH ' ELSE '' END;

RAISE DEBUG '%', (SELECT json_agg(row_to_json(e)) FROM elements e);

FOR v_cte IN 0 .. v_cte_max LOOP
    v_many_join_query := meta.u_enr_query_generate_distinct_many_join_query(in_source_id, v_cte);
    RAISE DEBUG 'Many-join query %', v_many_join_query;
    v_sql := v_sql || CASE WHEN v_cte > 0 THEN E')\n' ELSE '' END || 
       v_many_join_query ||
       CASE WHEN v_cte > 0 AND v_cte < v_cte_max OR v_many_join_query != '' AND v_cte < v_cte_max THEN ',' ELSE '' END ||
       CASE WHEN v_cte < v_cte_max THEN ' cte' || v_cte || ' AS ( ' ELSE '' END || 'SELECT ';
    -- Add raw/system attributes 
    IF v_cte = 0 THEN
        v_sql := v_sql || COALESCE((SELECT string_agg( e.expression || 
        CASE WHEN e.expression = 'T.' || e.alias THEN '' ELSE ' ' || e.alias END,', ') 
        FROM elements e WHERE e.type IN ('raw','system') AND cte = 0), '');

    ELSEIF v_cte = v_cte_max AND in_mode = 'input' THEN
        DELETE FROM elements WHERE type = 'system' AND alias = 's_input_id';
    END IF;

    -- Add attributes from prior CTEs
    v_sql := v_sql || COALESCE((SELECT string_agg('T.' || e.alias ,', ') 
    FROM elements e WHERE e.type IN ('raw','system','enrichment') AND e.cte < v_cte),'');
    -- Add current transits
    v_sql := v_sql || COALESCE(', ' || (SELECT string_agg(e.expression || ' ' || e.alias ,', ') 
    FROM elements e WHERE e.type like 'transit%' AND e.cte = v_cte
    ),'');
    -- Add prior transits required by downstream CTEs
    v_sql := v_sql || COALESCE(', ' || (SELECT string_agg('T.' || e.alias ,', ') 
    FROM elements e WHERE e.type like 'transit%' AND e.cte < v_cte
    AND EXISTS(SELECT 1 FROM elements fe WHERE fe.cte > v_cte AND fe.parent_ids @> ARRAY[e.id])),'');
   -- Add current CTE enrichments
    v_sql := v_sql || COALESCE(', ' || (SELECT string_agg( CASE WHEN e.data_type IS NOT NULL THEN 'CAST(' ELSE '' END ||
        e.expression || CASE WHEN e.data_type IS NOT NULL THEN ' AS ' || e.data_type || ')' ELSE '' END || ' ' || 
        CASE WHEN e.expression = 'T.' || e.alias THEN '' ELSE e.alias END ,', ') 
    FROM elements e WHERE e.type = 'enrichment' AND e.cte = v_cte),'');
    -- add FROM clause
    v_sql := v_sql || E'\nFROM ' || CASE WHEN v_cte = 0 THEN 'input' ELSE 'cte' || (v_cte - 1) END
        || ' T';
   -- Add current CTE joins
    v_sql := v_sql || COALESCE((SELECT  string_agg( E'\nLEFT JOIN ' || CASE WHEN in_source_id = e.source_id AND v_cte > 0 THEN 'cte' || (v_cte - 1) -- self-join
        ELSE meta.u_get_hub_table_name(e.source_id) END || ' ' || e.alias 
                       || ' ON ' || e.expression,' ' ORDER BY e.alias) 
    FROM elements e WHERE e.type = 'join' AND e.cte = v_cte),'');
   -- Add current CTE many-joins
    v_sql := v_sql || COALESCE((SELECT  string_agg( E'\nLEFT JOIN ' || e.alias || '_AGG' 
                       || ' ON ' || 
                       -- Aggregate join list
                       (SELECT string_agg('T.' || DL.att || ' = ' || e.alias || '_AGG.' || DL.att, ' AND ') 
                        FROM unnest(e.many_join_list) DL(att) )
                       ,' ') 
    FROM elements e WHERE e.type = 'many-join' AND e.cte = v_cte),'');


END LOOP;

/*
v_sql := v_sql || ')' || E'\nSELECT ';
v_sql := v_sql || COALESCE((SELECT string_agg('T.' || e.alias ,', ') 
FROM elements e WHERE e.type IN ('raw','system','enrichment')),'');
v_sql := v_sql || E'\nFROM cte' || v_cte_max;
*/

RETURN v_sql;
    

EXCEPTION WHEN OTHERS THEN
    RETURN 'QUERY GENERATION ERROR: ' || SQLERRM;

END;

$function$;

CREATE OR REPLACE FUNCTION meta.u_enr_query_generate_query(in_source_id int)
 RETURNS text
 LANGUAGE plpgsql
AS $function$

BEGIN
    RETURN meta.u_enr_query_generate_query(in_source_id, 'input', 0, '{}'::int[]);
END;

$function$;DROP FUNCTION IF EXISTS meta.u_enr_query_generate_sub_query( int,  text);
CREATE OR REPLACE FUNCTION meta.u_enr_query_get_enrichment_children(in_source_id int, in_enr_ids int[])
 RETURNS int[] -- returns array of child enrichment_ids
 LANGUAGE plpgsql
 COST 10
AS $function$
DECLARE
v_ret int[] = '{}';
v_next int[] := in_enr_ids; 
v_child_level int;

BEGIN
RAISE DEBUG 'u_enr_query_get_enrichment_children in_enr_ids %', in_enr_ids;
FOR v_child_level IN 0 .. 1000 LOOP

-- Add directly-dependent enrichments 
    v_next :=  
        (SELECT COALESCE(array_agg(parent_enrichment_id), '{}'::int[])
            FROM meta.enrichment_parameter p
            WHERE p.type = 'enrichment' AND ARRAY[p.enrichment_id] <@ v_next
            AND p.source_id = in_source_id)
        ||

-- Add enrichments from related sources where relation expression is an enrichment
	(SELECT COALESCE(array_agg(DISTINCT ep.parent_enrichment_id), '{}'::int[])
	FROM meta.enrichment e 
	JOIN meta.enrichment_parameter ep ON e.enrichment_id = ep.parent_enrichment_id
	JOIN meta.source_relation_parameter rp ON ARRAY[rp.source_relation_id] <@ ep.source_relation_ids
	WHERE (ep.source_id <> e.source_id OR ep.self_relation_container = 'Related') AND
	e.source_id = in_source_id AND ARRAY[rp.enrichment_id] <@ v_next);

    RAISE DEBUG 'v_next=%', v_next;
    EXIT WHEN cardinality(v_next) = 0;

    v_ret := v_ret || v_next;

    PERFORM meta.u_assert( v_child_level < 1000, 'Exceeded parameter recursion limit of 1000. in_enr_ids=' || in_enr_ids::text ||
    ' source_id=' || in_source_id);
    
END LOOP;


RETURN v_ret;        
END;

$function$;CREATE OR REPLACE FUNCTION meta.u_enr_query_get_related_source_ids(in_this_source_id int,
    in_relation_ids int[])
 RETURNS int[] -- returns array of related_source_ids
 LANGUAGE plpgsql
 COST 5
 PARALLEL SAFE
AS $function$
DECLARE
v_ret int[] := '{}';
v_related_source_id int;
v_source_id int;
i int;
BEGIN

PERFORM meta.u_assert(in_relation_ids IS NOT NULL, 'in_relation_ids parameter is NULL');

FOR i IN 1..array_upper(in_relation_ids,1) LOOP

	SELECT sr.source_id, sr.related_source_id
	INTO v_source_id, v_related_source_id
	FROM meta.source_relation sr
	WHERE sr.source_relation_id = in_relation_ids[i];

	PERFORM meta.u_assert( v_source_id IS NOT NULL, 'Non-existing source_relation_id=' || in_relation_ids[i] || ' detected in chain: ' || in_relation_ids::text);
	PERFORM meta.u_assert( in_this_source_id IN (v_source_id, v_related_source_id), 'Invalid source_relation_id=' || in_relation_ids[i] || ' detected in chain: ' || in_relation_ids::text);

	in_this_source_id := CASE WHEN in_this_source_id = v_source_id THEN v_related_source_id ELSE v_source_id END;
	v_ret := v_ret || in_this_source_id;
END LOOP;

RETURN v_ret;
END;

$function$;CREATE OR REPLACE FUNCTION meta.u_enr_query_get_relation_parameter_name(in_parameter meta.source_relation_parameter)
 RETURNS text -- returns attribute name
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
v_attribute_name text;
BEGIN
    IF in_parameter.type = 'enrichment' THEN
        PERFORM meta.u_assert( in_parameter.enrichment_id IS NOT NULL, 'enrichment_id is NULL for source_relation_parameter_id=' || in_parameter.source_relation_parameter_id);
        SELECT e.attribute_name INTO v_attribute_name
        FROM meta.enrichment e WHERE e.enrichment_id = in_parameter.enrichment_id;
        PERFORM meta.u_assert( v_attribute_name IS NOT NULL, 'enrichment_id=' || in_parameter.enrichment_id || 'referenced by source_relation_parameter_id' || in_parameter.source_relation_parameter_id || ' does not exist');
    ELSEIF in_parameter.type = 'raw' THEN
        PERFORM meta.u_assert( in_parameter.raw_attribute_id IS NOT NULL, 'raw_attribute_id is NULL for source_relation_parameter_id=' || in_parameter.source_relation_parameter_id);
        SELECT r.column_alias INTO v_attribute_name
        FROM meta.raw_attribute r WHERE r.raw_attribute_id = in_parameter.raw_attribute_id;
        PERFORM meta.u_assert( v_attribute_name IS NOT NULL, 'raw_attribute_id=' || in_parameter.raw_attribute_id || 'referenced by source_relation_parameter_id' || in_parameter.source_relation_parameter_id || ' does not exist');
    ELSEIF in_parameter.type = 'system' THEN
        PERFORM meta.u_assert( in_parameter.system_attribute_id IS NOT NULL, 'system_attribute_id is NULL for source_relation_parameter_id=' || in_parameter.source_relation_parameter_id);
        SELECT s.name INTO v_attribute_name
        FROM meta.system_attribute s WHERE s.system_attribute_id = in_parameter.system_attribute_id;
        PERFORM meta.u_assert( v_attribute_name IS NOT NULL, 'system_attribute_id=' || in_parameter.system_attribute_id || 'referenced by source_relation_parameter_id' || in_parameter.source_relation_parameter_id || ' does not exist');
    END IF;

RETURN v_attribute_name;
END;

$function$;CREATE OR REPLACE FUNCTION meta.u_enr_query_relation_alias(
    in_ids int[])
 RETURNS text -- returns array with last element removed
 LANGUAGE plpgsql
 COST 1
 IMMUTABLE PARALLEL SAFE
AS $function$
BEGIN


RETURN (
	SELECT string_agg( el::text, '_')
	FROM unnest(in_ids) el
	);
END;

$function$;CREATE OR REPLACE FUNCTION meta.u_enr_query_update_cte(
    in_cte int, in_mode text)
 RETURNS void
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
v_row_count int;
v_el meta.query_element;
v_sqa meta.query_element;
v_sqa_user meta.query_element;
v_sq_attributes int[];
v_transit meta.query_element;
BEGIN
-- recursively add cascading joins
LOOP
 UPDATE elements e SET cte = in_cte
 WHERE e.type = 'join' AND cte IS NULL
 AND e.parent_ids <@ (SELECT array_agg(ep.id) 
    FROM elements ep WHERE ep.cte <= in_cte 
    );
 GET DIAGNOSTICS v_row_count = ROW_COUNT;

 EXIT WHEN v_row_count = 0;
END LOOP;

-- Add all transits pointing to joins we just added
 UPDATE elements e SET cte = in_cte
 WHERE e.type = 'transit' AND cte IS NULL
 AND e.parent_ids <@ (SELECT array_agg(ep.id) FROM elements ep WHERE ep.cte <= in_cte);


-- Add all many-joins that can be resolved
FOR v_el IN SELECT * FROM elements e WHERE e.type = 'many-join' AND e.cte IS NULL 
    AND e.parent_ids <@ (SELECT array_agg(ep.id) 
    FROM elements ep WHERE ep.cte < in_cte 
   OR (ep.type IN ('system', 'raw') AND ep.cte = in_cte)
   OR (in_mode = 'recalculation' AND ep.type = 'enrichment' AND ep.cte = in_cte AND in_cte = 0 
    AND ep.expression = ('T.' || ep.alias) ) -- only for enrichments already calculated during enrichment process
    )
    LOOP
    RAISE DEBUG 'Processing CTE for many-join %', to_json(v_el);

    -- check if all many-join expressions can be resolved
    IF NOT EXISTS(
        SELECT 1 FROM elements sqa 
        WHERE sqa.type = 'many-join attribute'  
        AND sqa.parent_ids @> ARRAY[v_el.id] -- all expressions
        AND EXISTS(SELECT 1 FROM elements el WHERE el.id <> v_el.id AND sqa.parent_ids @> ARRAY[el.id] 
         AND el.cte is NULL) 
        ) THEN 
            -- move all many-join attributes to current CTE and capture element_ids
            RAISE DEBUG 'Moving many-join % into cte %', to_json(v_el), in_cte;

            WITH cu AS (
                UPDATE elements e SET cte = in_cte WHERE e.type = 'many-join attribute'  
                AND e.parent_ids @> ARRAY[v_el.id]
                RETURNING e.id
            ) SELECT array_agg(id) INTO v_sq_attributes
            FROM cu;
            -- move many-join to current CTE 
            UPDATE elements e 
            SET cte = in_cte
            WHERE e.id = v_el.id;
            
    END IF;
    END LOOP;

-- Add all transits pointing to many-joins we just added
 UPDATE elements e SET cte = in_cte
 WHERE e.type = 'transit-agg' AND cte IS NULL
 AND e.parent_ids <@ (SELECT array_agg(ep.id) FROM elements ep WHERE ep.cte <= in_cte);


-- Add all enrichments that can be resolved

 UPDATE elements e SET cte = in_cte
 WHERE e.type = 'enrichment' AND cte IS NULL
 AND e.parent_ids <@ (SELECT array_agg(ep.id) FROM elements ep 
 WHERE ep.cte < in_cte
 OR (ep.type IN ('system', 'transit', 'transit-agg', 'raw') AND ep.cte = in_cte)
 OR (in_mode = 'recalculation' AND ep.type = 'enrichment' AND ep.cte = in_cte AND in_cte = 0)
 );



-- Short-curcuit transits in current CTE 
FOR v_el IN SELECT * FROM elements e WHERE e.type IN ('transit','transit-agg') AND e.cte = in_cte 
    LOOP
        RAISE DEBUG 'Short-curcuiting transit %', to_json(v_el);

        UPDATE elements e
        SET expression = regexp_replace(e.expression,'(T\.' || v_el.alias || ')($|[^0-9])', v_el.expression || '\2','g')
        WHERE e.type in ('enrichment','many-join','many-join attribute')
        AND e.cte = in_cte
        AND e.expression ~ ('(T\.' || v_el.alias || ')($|[^0-9])');
        --AND meta.u_enr_query_find_in_parents(e.id,v_el.id);
    END LOOP;



-- Remove unused transits in current CTE
DELETE FROM elements e
WHERE e.type like 'transit%' AND e.cte = in_cte 
AND NOT EXISTS (SELECT 1 FROM elements ce WHERE ce.cte IS NULL AND ce.parent_ids @> ARRAY[e.id]);

--Add cte0 to unlabeled raw and system columns, these are columns that need derived values in cte0
IF in_cte = 0 THEN
    UPDATE elements e
    SET cte = 0
    WHERE e.type IN ('raw','system')
    AND cte IS NULL;
END IF;


END;

$function$;CREATE OR REPLACE FUNCTION meta.u_get_hub_table_name(in_source_id int, in_history_flag boolean = false)
 RETURNS text 
 LANGUAGE plpgsql
AS $function$

DECLARE
	v_table text;
	v_source meta.source;
BEGIN
	SELECT s.* INTO v_source FROM meta.source s WHERE s.source_id = in_source_id;

	IF v_source.refresh_type = 'unmanaged' THEN	
		RETURN v_source.ingestion_parameters->>'table_name';
	ELSE
		RETURN v_source.hub_view_name;
	END IF;
END;

$function$;-- creates single column select expression for the output query
CREATE OR REPLACE  FUNCTION meta.u_output_query_column_select(osc meta.output_source_column, in_hive_type text)
    RETURNS TEXT
    LANGUAGE plpgsql
AS
$function$

DECLARE
    v_attribute_name text;

BEGIN

    IF osc IS NULL THEN
        RETURN CASE in_hive_type
            WHEN 'struct' THEN 'struct()'
            WHEN 'array' THEN 'array()'
            ELSE 'CAST(null as ' || COALESCE(osc.datatype,'string') || ')'
            END;
    END IF;

    SELECT attribute_name INTO v_attribute_name
    FROM meta.u_get_output_parameter_name_id(osc);

    PERFORM meta.u_assert(v_attribute_name IS NOT NULL,format('Unable to lookup attribute name for output mapping %s',osc));

    RETURN (  COALESCE(osc.aggregate || '(' || CASE WHEN osc.aggregate_distinct_flag THEN ' DISTINCT ' ELSE '' END, '') 
        ||  COALESCE('J_' || array_to_string(osc.source_relation_ids, '_'), 'T') -- alias
                || '.' 
                || v_attribute_name -- column name
                || CASE WHEN COALESCE(osc.keys,'') <> '' THEN '.' || osc.keys ELSE '' END -- struct key path
        || CASE WHEN osc.aggregate IS NOT NULL THEN ')' ELSE '' END 
    );

END;
$function$;
CREATE OR REPLACE FUNCTION meta.imp_check_format(in_import_id int, in_format text)
    RETURNS void
    LANGUAGE plpgsql
AS
$function$
DECLARE
	v_major int;
    v_minor int;
    v_format_text_arr text[];
    v_format_spec text;
    v_current_major int := 2;
    v_current_minor int := 1;
BEGIN

    v_format_text_arr := regexp_match(in_format, '(^|core)(\d+)\.(\d+)$');
    v_format_spec = v_format_text_arr[1]; --  'core'
    v_major = v_format_text_arr[2]::int;
    v_minor = v_format_text_arr[3]::int;

    IF v_major IS NULL OR v_minor IS NULL OR v_format_spec NOT IN ('core') THEN
        RAISE EXCEPTION 'Invalid import format %', in_format;
    END IF;

        IF v_major <> 1 OR v_minor <> 0 THEN
            RAISE EXCEPTION 'Unsupported core format %', in_format;
        END IF;

    UPDATE meta.import SET  format = format('%s.%s',v_major, v_minor), 
                            format_spec = v_format_spec 
    WHERE import_id = in_import_id;

END;
$function$;CREATE OR REPLACE FUNCTION meta.imp_decode_relation(in_relation jsonb, in_project_id int)
    RETURNS jsonb
    LANGUAGE plpgsql
AS
$function$
DECLARE 
    v_ret jsonb := in_relation;
    v_decoded text[];
    v_source_id int;
    v_related_source_id int;
BEGIN

v_decoded := regexp_match(in_relation->>'name', '^\[([^]]+)]-(.+)-\[([^]]+)]$');

IF v_decoded IS NULL OR v_decoded[1] IS NULL OR v_decoded[2] IS NULL OR v_decoded[3] IS NULL THEN 
    RETURN v_ret || jsonb_build_object('error', 'Invalid relation name : ' || (in_relation->>'name'));
END IF;

v_source_id :=  meta.imp_map_source(v_decoded[1], in_project_id, false);
IF v_source_id IS NULL THEN 
    RETURN v_ret || jsonb_build_object('error', format('Source `%s` does not exist', v_decoded[1]));
END IF;

v_related_source_id :=  meta.imp_map_source(v_decoded[3], in_project_id, false);
IF v_related_source_id IS NULL THEN 
    RETURN v_ret || jsonb_build_object('error', format('Source `%s` does not exist', v_decoded[3]));
END IF;


v_ret := v_ret || jsonb_build_object('source_id', v_source_id)
               || jsonb_build_object('relation_name', v_decoded[2])
               || jsonb_build_object('related_source_id', v_related_source_id);

v_decoded := regexp_match(in_relation->>'cardinality', '^(M|1)-(M|1)$');

IF v_decoded IS NULL OR v_decoded[1] IS NULL OR v_decoded[2] IS NULL THEN 
    RETURN v_ret || jsonb_build_object('error', format('Invalid cardinality %s',in_relation->>'cardinality'));
END IF;

v_ret := v_ret || jsonb_build_object('source_cardinality',v_decoded[1]) || jsonb_build_object('related_source_cardinality',v_decoded[2]);

RETURN v_ret  - 'name' - 'cardinality';

END;
$function$;CREATE OR REPLACE FUNCTION meta.imp_map_relations(in_relation_uids jsonb)
    RETURNS int[]
    LANGUAGE plpgsql
AS
$function$
DECLARE 
    v_ret int[];
BEGIN
    IF in_relation_uids IS NULL THEN
        RETURN v_ret;
    END IF;
    SELECT COALESCE(array_agg(r.source_relation_id ORDER BY rn),'{}'::int[]) 
        INTO v_ret
        FROM jsonb_array_elements_text(in_relation_uids) WITH ORDINALITY a(el, rn)
        JOIN _imp_relation r ON r.relation_uid = a.el;
    -- check for unmapped
    PERFORM meta.u_assert(cardinality(v_ret) IS NOT DISTINCT FROM jsonb_array_length(in_relation_uids) ,'Unable to map relations ' || in_relation_uids::text);
    RETURN v_ret;
END;
$function$;CREATE OR REPLACE FUNCTION meta.imp_map_source(in_name text, in_project_id int, in_throw_error boolean = true)
    RETURNS int
    LANGUAGE plpgsql
AS
$function$
DECLARE
    v_ret int;
BEGIN
IF in_name IS NULL THEN
	RETURN null;
ELSE
	SELECT source_id INTO v_ret FROM meta.source WHERE source_name = in_name AND project_id = in_project_id;
    PERFORM meta.u_assert(NOT in_throw_error OR v_ret IS NOT NULL,'Source ' || in_name || ' does not exist in project ' || COALESCE(in_project_id::text,'NULL'));
	RETURN v_ret;
END IF;
END;
$function$;CREATE OR REPLACE FUNCTION meta.imp_parse_objects(in_import_id int)
    RETURNS void
    LANGUAGE plpgsql
AS
$function$
DECLARE
    v_stack text;
    v_log_id int;
BEGIN
    SELECT log_id INTO v_log_id FROM meta.import WHERE import_id = in_import_id;
    -- parse files and extract names
    WITH parsed AS (
        SELECT import_object_id, body_text::jsonb body
        FROM meta.import_object WHERE import_id = in_import_id)
    UPDATE meta.import_object io
    SET body = p.body, 
        name =  CASE WHEN object_type IN ('source' ,'output', 'group', 'token') THEN p.body->>(object_type || '_name')
                WHEN object_type IN ('output_template','source_template') THEN p.body->>'object_name'
        ELSE p.body->>'name' END,
        hash = md5(io.body_text)
    FROM parsed p WHERE p.import_object_id = io.import_object_id;

    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
    VALUES ( v_log_id, 'Import files parsing completed','imp_parse_objects', 'I', clock_timestamp());
END;
$function$;CREATE OR REPLACE FUNCTION meta.impc_execute(in_imp meta.import)
    RETURNS jsonb
    LANGUAGE plpgsql
AS
$function$
DECLARE
    v_err jsonb;
    v_count int;
BEGIN
     -- upsert changed objects in the order to maintain ref integrity
    -- update source_ids
    UPDATE meta.import_object io 
    SET id = s.source_id 
    FROM meta.source s
        WHERE io.import_id = in_imp.import_id AND s.project_id = in_imp.project_id
        AND s.source_name = io.name
        AND io.object_type = 'source';

    -- upsert sources part 1 and update ids in import_object table
    WITH us AS (
        SELECT meta.impc_upsert_source(io, in_imp) s
        FROM meta.import_object io 
        WHERE io.import_id = in_imp.import_id 
        AND io.object_type = 'source'
    )    
    SELECT jsonb_agg(s)
    INTO v_err
    FROM us
    WHERE s IS NOT NULL;

    IF v_err IS NOT NULL THEN
		RETURN v_err;
	END IF;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, format('Imported %s sources',v_count ),'impc_execute', 'I', clock_timestamp());

    UPDATE meta.import_object io
    SET id = s.source_id
    FROM meta.source s 
    WHERE io.import_id = in_imp.import_id AND io.id IS NULL AND io.object_type = 'source' AND io.name = s.source_name AND s.project_id = in_imp.project_id;

    -- delete all enr parameters in project
    DELETE FROM meta.enrichment_parameter ep
    WHERE ep.parent_enrichment_id IN (SELECT enrichment_id FROM meta.enrichment e 
            JOIN meta.source s ON s.source_id = e.source_id
            WHERE s.project_id = in_imp.project_id);

    -- delete all enr aggs in project
    DELETE FROM meta.enrichment_aggregation ea
    WHERE ea.enrichment_id IN (SELECT enrichment_id FROM meta.enrichment e 
            JOIN meta.source s ON s.source_id = e.source_id
            WHERE s.project_id = in_imp.project_id);

   -- delete relation parameters
    DELETE FROM meta.source_relation_parameter
    WHERE source_relation_id IN (SELECT source_relation_id FROM meta.source_relation sr
            JOIN meta.source s ON s.source_id = sr.source_id
            WHERE s.project_id = in_imp.project_id);

    -- delete mappings
    DELETE FROM meta.output_source_column osc
    WHERE osc.output_source_id IN (SELECT os.output_source_id FROM meta.output_source os JOIN meta.source s ON s.source_id = os.source_id
            WHERE s.project_id = in_imp.project_id);

    v_err = meta.impc_upsert_raw_attributes(in_imp);

    IF v_err IS NOT NULL THEN
		RETURN jsonb_build_object('error','raw attribute','error_detail', v_err);
	END IF;

    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, 'Imported raw attributes','svc_import_execute', 'I', clock_timestamp());

    -- create enrichment rules temp table
	CREATE TEMP TABLE _imp_enrichment ON COMMIT DROP AS 
	SELECT io.id source_id, io.name source_name, e.enrichment_id, r.rule, null::int parent_enrichment_id, r.rule->>'name' attribute_name
    FROM meta.import_object io CROSS JOIN jsonb_array_elements(io.body->'rules') r (rule)
	LEFT JOIN meta.enrichment e ON e.source_id = io.id AND e.attribute_name = r.rule->>'name' AND e.active_flag = COALESCE((r.rule->>'active_flag')::boolean, true)
    WHERE io.import_id = in_imp.import_id 
    AND io.object_type = 'source';

    SELECT jsonb_agg(jsonb_build_object('source_name',source_name,'attribute_name',attribute_name))
    INTO v_err
    FROM (SELECT source_name, rule->>'name' attribute_name
        FROM _imp_enrichment 
        GROUP BY source_name, rule->>'name' HAVING COUNT(1) > 1) dupes;
    
    IF v_err IS NOT NULL THEN
		RETURN jsonb_build_object('error','Duplicate rules','error_detail', v_err);
	END IF;

    SELECT jsonb_agg(jsonb_build_object('source_name',source_name,'attribute_name',rule->>'name'))
    INTO v_err
    FROM _imp_enrichment
    WHERE NOT rule->>'name' ~ '^[a-z_]+[a-z0-9_]*$';

    IF v_err IS NOT NULL THEN
		RETURN jsonb_build_object('error','Invalid rule name(s). Name has to start with lowercase letter or _ It may contain lowercase letters, numbers and _',
        'error_detail', v_err);
	END IF;

    UPDATE _imp_enrichment
    SET enrichment_id = nextval('meta.seq_enrichment'::regclass)::int
    WHERE enrichment_id IS NULL;

    PERFORM meta.impc_upsert_enrichments(in_imp);

    --upsert relations, relation parameters
    v_err := meta.impc_upsert_relations(in_imp);
    IF v_err IS NOT NULL THEN
		RETURN  v_err;
	END IF;

    -- upsert enrichment parameters
    v_err :=  meta.impc_upsert_enrichment_parameters(in_imp);
    IF v_err IS NOT NULL THEN
		RETURN  v_err;
	END IF;

    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, 'Imported enrichment parameters','svc_import_execute', 'I', clock_timestamp());

    -- update output ids
    UPDATE meta.import_object io 
    SET id = o.output_id
    FROM meta.output o
    WHERE io.import_id = in_imp.import_id AND io.id IS NULL AND io.object_type = 'output' AND io.name = o.output_name AND o.project_id = in_imp.project_id;

    -- upsert outputs
    WITH uo AS (
        SELECT meta.impc_upsert_output(io, in_imp) o
        FROM meta.import_object io 
        WHERE io.import_id = in_imp.import_id 
        AND io.object_type = 'output'
    )
    SELECT json_agg(o)
    INTO v_err
    FROM uo
    WHERE o IS NOT NULL;

    IF v_err IS NOT NULL THEN
		RETURN v_err;
	END IF;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, format('Imported %s outputs',v_count),'svc_import_execute', 'I', clock_timestamp());

    UPDATE meta.import_object io
    SET id = o.output_id
    FROM meta.output o
    WHERE io.import_id = in_imp.import_id AND io.id IS NULL AND io.object_type = 'output' AND io.name = o.output_name AND o.project_id = in_imp.project_id;

    -- upsert channels
    v_err := meta.impc_upsert_channels(in_imp);

    IF v_err IS NOT NULL THEN
		RETURN v_err;
	END IF;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
    VALUES ( in_imp.log_id, format('Deleted %s objects not existing in import',v_count),'svc_import_execute', 'I', clock_timestamp());

    -- validate all imported enrichment parameters
    WITH c AS (
        SELECT e.enrichment_id, e.attribute_name, e.source_id, meta.u_validate_expression_parameters(e) val
        FROM meta.enrichment e JOIN _imp_enrichment ie ON e.enrichment_id = ie.enrichment_id
    )
    SELECT jsonb_agg(jsonb_build_object('attribute_name',c.attribute_name, 'source_name',s.source_name, 'error', val))
        INTO v_err 
    FROM c JOIN meta.source s ON s.source_id = c.source_id
    WHERE val IS DISTINCT FROM '';
    
    IF v_err IS NOT NULL THEN 
        RETURN jsonb_build_object('error' , 'Rule validation errors', 'error_detail', v_err );
    END IF;
    
    -- validate all imported output mappings
    WITH c AS (
        SELECT meta.u_validate_output_mapping(om) val
        FROM  meta.output_source_column om 
	    JOIN _imp_mapping im ON om.output_source_id = im.output_source_id 
    )
    SELECT jsonb_agg(val)
        INTO v_err 
    FROM c 
    WHERE val IS DISTINCT FROM '';
    
    IF v_err IS NOT NULL THEN 
        RETURN jsonb_build_object('error' , 'Output mapping validation errors', 'error_detail', v_err );
    END IF;
    
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, 'Import files parsed successfully. ','impc_execute', 'I', clock_timestamp());

    RETURN null;    
END;
$function$;
CREATE OR REPLACE FUNCTION meta.impc_parse_mapping(in_channel jsonb, in_mapping text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
AS $BODY$

DECLARE
    v_parsed text[];
    v_aggregates text;
    v_attribute_name text;
    v_column_name text;
    v_aggregate text;
    v_keys text;
    v_ret jsonb = '{}'::jsonb;
    v_distinct_flag boolean;
    v_parameter parameter_map;
    v_output_column_id int;
BEGIN

    SELECT string_agg(aggregate_name,'|') INTO v_aggregates
    FROM meta.aggregate;

    in_mapping := trim(in_mapping);

    -- check for aggregate
    v_parsed = regexp_match(in_mapping,format('^(%s)\((distinct\s+)?(?:\[This\]\.)?(\w+)((?:\.\w+)+)?\)\s+(\w+)', v_aggregates),'i');

    IF v_parsed[1] IS NOT NULL THEN -- aggregate expression

        IF in_channel->>'operation_type' != 'Aggregate' THEN
            RETURN jsonb_build_object('error', format('Aggregation in output `%s` channel `%s` expression: %s. Please add operation_type: Aggregate to channel definition', in_channel->>'output_name', in_channel->>'output_source_name', in_mapping)); 
        END IF;

        v_aggregate := lower(v_parsed[1]);
        v_distinct_flag = v_parsed[2] ~* 'distinct\s+';	
        v_attribute_name = lower(v_parsed[3]);
        v_keys = v_parsed[4];
        v_column_name = v_parsed[5];
    ELSE -- no aggregation
        v_parsed = regexp_match(in_mapping,'^(?:\[This\]\.)?(\w+)((?:\.\w+)+)?\s+(\w+)$','i');
        v_attribute_name = lower(v_parsed[1]);
        v_keys = v_parsed[2];
        v_column_name = v_parsed[3];
    END IF;

    IF v_attribute_name IS NULL OR v_column_name IS NULL THEN
        RETURN jsonb_build_object('error', format('Invalid mapping in output `%s` channel `%s` expression: %s', in_channel->>'output_name', in_channel->>'output_source_name', in_mapping)); 
    END IF;

    v_parameter := meta.u_lookup_source_attribute((in_channel->'source_id')::int, v_attribute_name);

    IF v_parameter.error IS NOT NULL THEN
        RETURN jsonb_build_object('error', format('Invalid attribute in output `%s` channel `%s` expression: %s Details: %s', in_channel->>'output_name', in_channel->>'output_source_name', in_mapping, v_parameter.error)); 
    END IF;

    SELECT output_column_id INTO v_output_column_id
    FROM meta.output_column oc 
    WHERE oc.output_id = (in_channel->>'output_id')::int AND oc.name = v_column_name;

    IF v_output_column_id IS NULL THEN
        RETURN jsonb_build_object('error', format('Output column `%s` does not exists in output `%s` channel `%s` expression: %s output_id=%s',v_column_name, in_channel->>'output_name', in_channel->>'output_source_name', in_mapping, in_channel->>'output_id')); 
    END IF;


    RETURN to_jsonb(v_parameter) || jsonb_build_object('expression', v_attribute_name || COALESCE(v_keys,''), 'aggregate', v_aggregate, 'aggregate_distinct_flag', v_distinct_flag
     , 'keys', v_keys, 'output_column_id', v_output_column_id);

END;

$BODY$;

CREATE OR REPLACE FUNCTION meta.impc_parse_raw_attribute(in_raw jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
AS $BODY$

DECLARE
    v_parsed text[];
    v_attribute_name text;
    v_data_type text;
    v_datatype_schema jsonb;
BEGIN

    IF jsonb_typeof(in_raw) = 'string' THEN -- parse string
        v_parsed := regexp_split_to_array(in_raw->>0, '\s+(AS\s+|)','i');
        v_attribute_name := v_parsed[1];
        v_data_type := lower(v_parsed[2]);
        v_datatype_schema := meta.u_get_schema_from_type(null, v_data_type);

    ELSEIF jsonb_typeof(in_raw) = 'object' THEN -- parse object
        v_attribute_name := in_raw->>'name';
        v_datatype_schema := in_raw->'schema';
        v_data_type := meta.u_get_typename_from_schema(v_datatype_schema);
    ELSE
        RETURN jsonb_build_object('error', format('Invalid raw attribute spec %s', in_raw));
    END IF;

-- validate name and type

    IF v_attribute_name IS NULL THEN
        RETURN jsonb_build_object('error', format('Invalid raw attribute %s', in_raw));
    END IF;

    IF v_data_type NOT IN (SELECT hive_type from meta.attribute_type) THEN
        RETURN jsonb_build_object('error', format('Invalid raw attribute datatype %s', in_raw));
    END IF;

    RETURN jsonb_build_object('name', lower(v_attribute_name), 'data_type', v_data_type, 'datatype_schema', v_datatype_schema);

END;

$BODY$;

CREATE OR REPLACE FUNCTION meta.impc_parse_relation(in_parameters jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
AS $BODY$

DECLARE
    v_expression text := in_parameters->>'expression';
    v_expression_parsed text := v_expression;
    v_field text;
    v_source_id int := (in_parameters->>'source_id')::int;
    v_related_source_id int := (in_parameters->>'related_source_id')::int;
    v_insert_param_id int;
    v_parameter parameter_map;


BEGIN
    IF v_source_id IS NULL THEN
        RETURN jsonb_build_object('error', 'source_id is NULL'); 
    END IF;

    IF v_related_source_id IS NULL THEN
        RETURN jsonb_build_object('error', 'v_related_source_id is NULL'); 
    END IF;

    FOR v_field IN SELECT unnest(t.f) FROM regexp_matches(v_expression, '(?:\[This]\.)(\w+)', 'g') t(f) LOOP
        
        v_parameter := meta.u_lookup_source_attribute(v_source_id, v_field);

        IF v_parameter.error IS NOT NULL THEN
            RETURN jsonb_build_object('error',format('Invalid field [This].%s in relation expression. %s',v_field, v_parameter.error));
        END IF;

        v_insert_param_id := meta.u_insert_source_relation_parameters(v_parameter, (in_parameters->'source_relation_id')::int, true);
        
        v_expression := regexp_replace(v_expression,'(\[This]\.' || v_field || ')([^\w]+|$)',
            meta.u_datatype_test_expression(v_parameter.datatype, v_parameter.datatype_schema) || '\2','g');

        v_expression_parsed := regexp_replace(v_expression_parsed,'(\[This]\.' || v_field || ')([^\w]+|$)',
            'P<' || v_insert_param_id || '>\2','g');
    END LOOP;


    FOR v_field IN SELECT unnest(t.f) FROM regexp_matches(v_expression, '(?:\[Related]\.)(\w+)', 'g') t(f) LOOP
        
        v_parameter := meta.u_lookup_source_attribute(v_related_source_id, v_field);

        IF v_parameter.error IS NOT NULL THEN
            RETURN jsonb_build_object('error',format('Invalid field [Related].%s in relation expression. %s',v_field, v_parameter.error));
        END IF;

        v_insert_param_id := meta.u_insert_source_relation_parameters(v_parameter, (in_parameters->'source_relation_id')::int, false);
        
        v_expression := regexp_replace(v_expression,'(\[Related]\.' || v_field || ')([^\w]+|$)',
            meta.u_datatype_test_expression(v_parameter.datatype, v_parameter.datatype_schema) || '\2','g');

        v_expression_parsed := regexp_replace(v_expression_parsed,'(\[Related]\.' || v_field || ')([^\w]+|$)',
            'P<' || v_insert_param_id || '>\2','g');

    END LOOP;

    RETURN json_build_object('test_expression', 'SELECT ' || v_expression || ' as col1 FROM datatypes'
    , 'expression_parsed', v_expression_parsed);

END;

$BODY$;

CREATE OR REPLACE FUNCTION meta.impc_test_expressions(in_import_id int)
    RETURNS json
    LANGUAGE plpgsql
AS
$function$

DECLARE

    v_project_id int;

BEGIN

SELECT project_id INTO v_project_id FROM meta.import WHERE import_id = in_import_id;

    RETURN (
        SELECT COALESCE(json_agg(t),'[]'::json) FROM (
            SELECT json_build_object('source_relation_id',source_relation_id, 'expression', expression) t
            FROM meta.relation_test t
            WHERE t.project_id = v_project_id AND t.expression IS NOT NULL AND result IS NULL
            UNION all
            SELECT json_build_object('enrichment_id',enrichment_id, 'expression', expression) t
            FROM meta.enrichment_test t 
            WHERE t.project_id = v_project_id AND expression IS NOT NULL AND result IS NULL
            UNION all
            SELECT json_build_object('output_source_id',output_source_id, 'expression', expression) t
            FROM meta.output_filter_test t 
            WHERE t.project_id = v_project_id AND expression IS NOT NULL AND result IS NULL
        ) t
    );
     
END;
$function$;CREATE OR REPLACE FUNCTION meta.impc_update_test_results(in_import_id int, in_res json)
    RETURNS json
    LANGUAGE plpgsql
AS
$function$
DECLARE 
    v_err jsonb;
    v_imp meta.import;
    v_next json;

BEGIN

SELECT * INTO v_imp FROM meta.import WHERE import_id = in_import_id;

CREATE TEMP TABLE _test_result ON COMMIT DROP AS
SELECT (e->>'source_relation_id')::int source_relation_id, (e->>'enrichment_id')::int enrichment_id, (e->>'output_source_id')::int output_source_id,  e->'result' result
FROM json_array_elements(COALESCE(in_res,'[]'::json)) e;

           
UPDATE meta.relation_test t
SET result = r.result
FROM _test_result r 
WHERE t.source_relation_id = r.source_relation_id AND t.project_id = v_imp.project_id;

UPDATE meta.enrichment_test t
SET result = r.result
FROM _test_result r 
WHERE t.enrichment_id = r.enrichment_id AND t.project_id = v_imp.project_id;

UPDATE meta.output_filter_test t
SET result = r.result
FROM _test_result r 
WHERE t.output_source_id = r.output_source_id AND t.project_id = v_imp.project_id;


-- check relation errors
SELECT json_agg(jsonb_build_object('name', format('[%s]-%s-[%s]',s.source_name, sr.relation_name, rs.source_name), 'error', COALESCE(t.result->'expression_error', t.result) ))
INTO v_err
FROM meta.relation_test t
LEFT JOIN ( meta.source_relation sr 
    JOIN meta.source s ON sr.source_id = s.source_id 
    JOIN meta.source rs ON sr.related_source_id = rs.source_id 
    ) ON sr.source_relation_id = t.source_relation_id
WHERE t.project_id = v_imp.project_id AND t.source_relation_id IS NOT NULL AND t.result IS NOT NULL AND t.result->>'data_type' IS DISTINCT FROM 'boolean';

IF v_err IS NOT NULL THEN
    PERFORM meta.svc_import_complete(in_import_id, 'F', format('Invalid relation expressions: %s', v_err));
    RETURN json_build_object('error',true);
END IF;

-- check enrichment errors
SELECT json_agg(jsonb_build_object('rule_name', e.attribute_name, 'source_name', s.source_name, 'error', COALESCE(t.result->'expression_error', t.result) ))
INTO v_err
FROM meta.enrichment_test t
LEFT JOIN meta.enrichment e ON e.enrichment_id = t.enrichment_id
LEFT JOIN meta.source s ON e.source_id = s.source_id 
WHERE t.project_id = v_imp.project_id AND t.enrichment_id IS NOT NULL AND t.result IS NOT NULL AND t.result->>'data_type' IS NULL;

IF v_err IS NOT NULL THEN
    PERFORM meta.svc_import_complete(in_import_id, 'F', format('Invalid rule expressions: %s', v_err));
    RETURN json_build_object('error',true);
END IF;


-- check output channel filter errors
SELECT json_agg(jsonb_build_object('source_name', s.source_name, 'output_name', o.output_name, 'error', COALESCE(t.result->'expression_error', t.result) ))
INTO v_err
FROM meta.output_filter_test t
LEFT JOIN meta.output_source os ON os.output_source_id = t.output_source_id
LEFT JOIN meta.source s ON os.source_id = s.source_id 
LEFT JOIN meta.output o ON o.output_id = os.output_id
WHERE t.project_id = v_imp.project_id AND t.output_source_id IS NOT NULL AND t.result IS NOT NULL AND t.result->>'data_type' IS DISTINCT FROM 'boolean';

IF v_err IS NOT NULL THEN
    PERFORM meta.svc_import_complete(in_import_id, 'F', format('Invalid output filter expressions: %s', v_err));
    RETURN json_build_object('error',true);
END IF;


-- update enrichment data types
UPDATE meta.enrichment e
SET datatype = t.result->>'data_type', datatype_schema = t.result->'schema'
FROM meta.enrichment_test t  
WHERE t.project_id = v_imp.project_id AND t.enrichment_id = e.enrichment_id AND e.datatype IS NULL;

-- generate test expressions
UPDATE meta.relation_test t
SET expression = meta.u_build_datatype_test_expr_from_parsed(sr)
FROM meta.source_relation sr  
WHERE t.source_relation_id = sr.source_relation_id AND t.project_id = v_imp.project_id AND t.expression IS NULL;

UPDATE meta.enrichment_test t
SET expression = meta.u_build_datatype_test_expr_from_parsed(e)
FROM meta.enrichment e
WHERE t.enrichment_id = e.enrichment_id AND t.project_id = v_imp.project_id AND t.expression IS NULL;

UPDATE meta.output_filter_test t
SET expression = (meta.svc_parse_enrichment(json_build_object('enrichment',json_build_object('expression', os.filter, 'source_id', os.source_id)),  
	in_mode => 'import'))->>'expression'
FROM meta.output_source os
WHERE t.output_source_id = os.output_source_id AND t.project_id = v_imp.project_id AND t.expression IS NULL;


v_next := meta.impc_test_expressions(in_import_id);

IF v_next::text = '[]' THEN   
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( v_imp.log_id, 'Expressions validated, Import completed successfully. ','impc_execute', 'I', clock_timestamp());
    UPDATE meta.import SET status_code = 'P' WHERE import_id = v_imp.import_id;   
END IF;

RETURN json_build_object('next',v_next);

END;
$function$;CREATE OR REPLACE FUNCTION meta.impc_upsert_channels(in_imp meta.import)
    RETURNS jsonb
    LANGUAGE plpgsql
AS
$function$
DECLARE
	v_err jsonb;
	v_count int;
BEGIN

	CREATE TEMP TABLE _imp_channel ON COMMIT DROP AS 
	WITH ct AS (
		SELECT io.id output_id, io.name output_name, s.source_id , c.el->>'source_name' output_source_name 
			, COALESCE(c.el->>'operation_type','none') operation_type, c.el->>'filter' "filter", c.el->'mappings' mappings
			, ROW_NUMBER() OVER (PARTITION BY io.id, source_id) rn
		FROM meta.import_object io CROSS JOIN jsonb_array_elements(io.body->'channels') c(el)
		LEFT JOIN meta.source s ON s.source_name = c.el->>'source_name' AND s.project_id = in_imp.project_id
		WHERE io.import_id = in_imp.import_id 
		AND io.object_type = 'output'
	)
	SELECT output_id, output_name, source_id , output_source_name || CASE WHEN rn > 1 THEN ' ' || rn ELSE '' END output_source_name -- uniquefy channel names
		, null::int output_source_id, null::json filter_parsed, operation_type, filter, mappings
	FROM ct;

	-- check source match
	SELECT jsonb_agg(jsonb_build_object('output_name',output_name, 'source_name',output_source_name)) 
	INTO v_err
	FROM _imp_channel WHERE source_id IS NULL;

	IF v_err IS NOT NULL THEN
		RETURN jsonb_build_object('error','Cannot match output channel sources','error_detail', v_err);
	END IF;

	UPDATE _imp_channel c SET filter_parsed = meta.svc_parse_enrichment(json_build_object('enrichment',json_build_object('expression', c.filter, 'source_id', c.source_id)),  
	in_mode => 'import')
	WHERE c.filter IS NOT NULL;

	-- check errors
	SELECT jsonb_agg(jsonb_build_object('source_name', c.output_source_name, 'output_name', c.output_name, 'error', c.filter_parsed->>'error'))
	INTO v_err
	FROM _imp_channel c 
	WHERE c.filter_parsed->>'error' IS NOT NULL;

	IF v_err IS NOT NULL THEN
		RETURN  jsonb_build_object('error','Filter expression parsing errors','error_detail',v_err);
	END IF;

	-- update id
	UPDATE _imp_channel SET output_source_id = nextval('meta.seq_output_source'::regclass)::int;

	-- insert channels
	INSERT INTO meta.output_source(output_source_id, source_id ,output_id ,filter ,operation_type ,active_flag ,created_userid ,create_datetime ,output_package_parameters ,output_source_name ,include_pass_flag ,include_fail_flag ,include_warn_flag ,description )
	SELECT c.output_source_id, c.source_id, c.output_id, replace(c.filter, '[This].', ''), c.operation_type, COALESCE(j.active_flag,true), 'Import ' || in_imp.import_id, 
	in_imp.create_datetime, j.output_package_parameters, c.output_source_name, COALESCE(j.include_pass_flag,true), COALESCE(j.include_fail_flag,false), COALESCE(j.include_warn_flag,false), j.description
	FROM _imp_channel c
	JOIN meta.output o ON o.output_id = c.output_id
	CROSS JOIN jsonb_populate_record(null::meta.output_source, to_jsonb(c)) j;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, format('Inserted %s channels', v_count),'svc_import_execute', 'I', clock_timestamp());


	-- build mapping table
	CREATE TEMP TABLE _imp_mapping ON COMMIT DROP AS	
	SELECT c.output_source_id, mapping
	FROM _imp_channel c 
	CROSS JOIN jsonb_array_elements_text(c.mappings) el
    CROSS JOIN meta.impc_parse_mapping(to_jsonb(c), el) mapping;

	-- check columns mapping
	SELECT jsonb_agg(mapping->'error') 
	INTO v_err
	FROM _imp_mapping WHERE mapping->>'error' IS NOT NULL;

	IF v_err IS NOT NULL THEN
		RETURN  jsonb_build_object('error','Mapping errors','error_detail',v_err);
	END IF;

	INSERT INTO meta.output_source_column(output_source_id ,output_column_id ,expression ,datatype ,
	type ,enrichment_id ,raw_attribute_id ,system_attribute_id , source_relation_ids ,create_datetime ,created_userid, aggregate ,aggregate_distinct_flag, keys )
	SELECT m.output_source_id ,j.output_column_id ,j.expression ,j.datatype ,
	j.type ,j.enrichment_id ,j.raw_attribute_id ,j.system_attribute_id , null ,in_imp.create_datetime, 'Import ' || in_imp.import_id, j.aggregate ,j.aggregate_distinct_flag, j.keys 
	FROM _imp_mapping m
	CROSS JOIN jsonb_populate_record(null::meta.output_source_column, m.mapping) j;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, format('Inserted %s column mappings', v_count),'svc_import_execute', 'I', clock_timestamp());

-- save test expressions
	DELETE FROM meta.output_filter_test WHERE project_id = in_imp.project_id;
-- save test expressions for filter expressions that can be resolved (point to raw + system attributes)
	INSERT INTO meta.output_filter_test (output_source_id, project_id, expression)
	SELECT c.output_source_id, in_imp.project_id, c.filter_parsed->>'expression'
	FROM _imp_channel c 
	WHERE c.filter IS NOT NULL; 	

	RETURN NULL;	
				  
END;
$function$;CREATE OR REPLACE FUNCTION meta.impc_upsert_enrichment_parameters(in_imp meta.import)
    RETURNS jsonb
    LANGUAGE plpgsql
AS
$function$
DECLARE
	v_err jsonb;
BEGIN

    -- create relation mapping table
    CREATE TEMP TABLE _imp_relation ON COMMIT DROP AS
    SELECT sr.source_relation_id, format('[%s]-%s-[%s]', s.source_name, sr.relation_name, rs.source_name) relation_uid
    FROM meta.source_relation sr
    JOIN meta.source s ON sr.source_id = s.source_id AND s.project_id = in_imp.project_id
    JOIN meta.source rs ON sr.related_source_id = rs.source_id 
    WHERE sr.active_flag;

    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, 'Created relation mapping table','svc_import_execute', 'I', clock_timestamp());

	-- check for invalid relations in rules
	SELECT jsonb_agg(jsonb_build_object('source_name', s.source_name, 'rule_name', e.attribute_name, 'relation', rel))
	INTO v_err
	FROM _imp_enrichment e 
	CROSS JOIN jsonb_array_elements(e.rule->'parameters') p
	CROSS JOIN jsonb_array_elements_text(p->'relations') rel 
    LEFT JOIN _imp_relation r ON r.relation_uid = rel
	LEFT JOIN meta.source s ON s.source_id = e.source_id
	WHERE r.relation_uid IS NULL;

    IF v_err IS NOT NULL THEN
		RETURN  jsonb_build_object('error','Invalid relation paths in rule parameters','error_detail',v_err);
	END IF;

	-- check for invalid source names in rule parameters
	SELECT jsonb_agg(jsonb_build_object('source_name', s.source_name, 'rule_name', e.attribute_name, 'parameter_source_name', p->>'source_name'))
	INTO v_err
	FROM _imp_enrichment e 
	CROSS JOIN jsonb_array_elements(e.rule->'parameters') p
	LEFT JOIN meta.source s ON s.source_id = e.source_id
    LEFT JOIN meta.source ps ON ps.source_name = p->>'source_name' AND ps.project_id = in_imp.project_id
	WHERE p->>'source_name' IS NOT NULL AND ps.source_id IS NULL;

    IF v_err IS NOT NULL THEN
		RETURN  jsonb_build_object('error','Invalid source names in rule parameters','error_detail',v_err);
	END IF;

    -- check for duplicate source names in rule parameters
	SELECT jsonb_agg(jsonb_build_object('source_name', s.source_name, 'rule_name', e.attribute_name, 'parameter_source_name', p->>'source_name'))
	INTO v_err
	FROM _imp_enrichment e 
	CROSS JOIN jsonb_array_elements(e.rule->'parameters') p
	LEFT JOIN meta.source s ON s.source_id = e.source_id
    GROUP BY s.source_name, e.attribute_name, p->>'source_name' HAVING COUNT(1) > 1;


    IF v_err IS NOT NULL THEN
		RETURN  jsonb_build_object('error','Duplicate relation paths in rule parameters','error_detail',v_err);
	END IF;

    -- check for blank source names in rule parameters
	SELECT jsonb_agg(jsonb_build_object('source_name', s.source_name, 'rule_name', e.attribute_name))
	INTO v_err
	FROM _imp_enrichment e 
	CROSS JOIN jsonb_array_elements(e.rule->'parameters') p
	LEFT JOIN meta.source s ON s.source_id = e.source_id
    WHERE p->>'source_name' IS NULL
    AND (SELECT COUNT(DISTINCT m[1])
        FROM regexp_matches(e.rule->>'expression', '\[([^\]]+)].\w+','g') m
        WHERE m[1] !~ 'This|\d+' -- exclude This and array indexes
    ) > 1;

    IF v_err IS NOT NULL THEN
		RETURN  jsonb_build_object('error','When rule expression references multiple sources, rule parameter is required to have `source_name`','error_detail',v_err);
	END IF;

	-- build enrichment parameter table
    CREATE TEMP TABLE _impc_enrichment_parameter ON COMMIT DROP AS 
    SELECT e.enrichment_id,  p->>'source_name' source_name, meta.imp_map_relations(p->'relations') source_relation_ids
    FROM _imp_enrichment e
    CROSS JOIN jsonb_array_elements(e.rule->'parameters') p;


	-- parse enrichments
    CREATE TEMP TABLE _impc_enrichment_parsed ON COMMIT DROP AS 
	SELECT e.enrichment_id, p->>'expression' expression, p->'enrichment' enrichment, p->'params' params, p->'aggs' aggs, p->>'error' error
	FROM _imp_enrichment ie
	JOIN meta.enrichment e ON e.enrichment_id = ie.enrichment_id
	CROSS JOIN meta.svc_parse_enrichment(json_build_object('enrichment',e), in_mode => 'import') p; -- save test expressions for rule expressions that can be resolved (point to raw + system attributes)

	-- check errors
	SELECT jsonb_agg(jsonb_build_object('source_name', s.source_name, 'rule_name', e.attribute_name, 'error', ep.error))
	INTO v_err
	FROM _impc_enrichment_parsed ep
	JOIN _imp_enrichment e ON ep.enrichment_id = e.enrichment_id
	LEFT JOIN meta.source s ON s.source_id = e.source_id
	WHERE ep.error IS NOT NULL;

	IF v_err IS NOT NULL THEN
		RETURN  jsonb_build_object('error','Rule expression parsing errors','error_detail',v_err);
	END IF;

	-- check parameter errors
	SELECT jsonb_agg(jsonb_build_object('source_name', s.source_name, 'rule_name', e.attribute_name, 
	'parameter', format('[%s].%s', p->>'source_name', p->>'attribute_name'), 'error', p->'paths'->>'error'))
	INTO v_err
	FROM _impc_enrichment_parsed ep
	JOIN _imp_enrichment e ON ep.enrichment_id = e.enrichment_id
	LEFT JOIN meta.source s ON s.source_id = e.source_id
	CROSS JOIN json_array_elements(ep.params) p
	WHERE p->'paths'->>'error' IS NOT NULL;

	IF v_err IS NOT NULL THEN
		RETURN  jsonb_build_object('error','Rule relations errors','error_detail',v_err);
	END IF;

	-- save parameters
	INSERT INTO meta.enrichment_parameter(enrichment_parameter_id, parent_enrichment_id, type, enrichment_id, raw_attribute_id, system_attribute_id, source_id, 
	source_relation_ids, self_relation_container, create_datetime, aggregation_id)
	SELECT (par->>'id')::int, ep.enrichment_id, p.type, p.enrichment_id, p.raw_attribute_id, p.system_attribute_id, p.source_id, 
    p.source_relation_ids, CASE WHEN par->>'source_name' <> 'This' AND e.source_id = p.source_id THEN 'Related' END self_relation_container, now(), p.aggregation_id
	FROM _impc_enrichment_parsed ep 
	JOIN _imp_enrichment e ON ep.enrichment_id = e.enrichment_id
	CROSS JOIN json_array_elements(ep.params) par
	CROSS JOIN json_populate_record(null::meta.enrichment_parameter,par) p;

	-- save aggregates
	INSERT INTO meta.enrichment_aggregation(enrichment_aggregation_id, enrichment_id, expression, function, relation_ids, create_datetime)
	SELECT (a->>'id')::int, ep.enrichment_id, a->>'expression_parsed', a->>'function', meta.u_json_array_to_int_array(a->'relation_ids'), now() 
	FROM _impc_enrichment_parsed ep 
	CROSS JOIN json_array_elements(ep.aggs) a;

	-- save test expressions
	DELETE FROM meta.enrichment_test WHERE project_id = in_imp.project_id;

	INSERT INTO meta.enrichment_test (enrichment_id, project_id, expression)
	SELECT enrichment_id, in_imp.project_id, expression
	FROM _impc_enrichment_parsed ep; 

	-- update expression_parsed
	UPDATE meta.enrichment e
	SET expression_parsed = ep.enrichment->>'expression_parsed'
	FROM _impc_enrichment_parsed ep
	WHERE e.enrichment_id = ep.enrichment_id;

	RETURN null;
	  
END;
$function$;CREATE OR REPLACE FUNCTION meta.impc_upsert_enrichments(in_imp meta.import)
    RETURNS void
    LANGUAGE plpgsql
AS
$function$
DECLARE
    v_count int;
BEGIN
    -- delete rules that don't exist in import

    DELETE FROM meta.enrichment e 
    WHERE e.source_id IN (SELECT s.source_id FROM meta.source s WHERE s.project_id = in_imp.project_id)
    AND e.enrichment_id NOT IN (SELECT enrichment_id FROM  _imp_enrichment );

    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, format('Deleted %s enrichments', v_count),'imp_upsert_enrichments', 'I', clock_timestamp());

    --INSERT changed enrichments (includes new)
	INSERT INTO meta.enrichment(enrichment_id, parent_enrichment_id ,source_id ,name ,description, cast_datatype ,attribute_name ,expression ,rule_type_code ,validation_action_code ,validation_type_code ,keep_current_flag ,window_function_flag ,unique_flag ,active_flag ,create_datetime ,created_userid)
	SELECT ie.enrichment_id, ie.parent_enrichment_id, ie.source_id,  j.name, j.description, COALESCE(j.cast_datatype,''), COALESCE(j.attribute_name,j.name) attribute_name, j.expression, COALESCE(j.rule_type_code,'E'), j.validation_action_code, j.validation_type_code, COALESCE(j.keep_current_flag, j.expression ~* 'over\s*\(.*\)'), COALESCE(j.window_function_flag,false), COALESCE(j.unique_flag,false), COALESCE(j.active_flag,true), in_imp.create_datetime, 'Import ' || in_imp.import_id
	FROM _imp_enrichment ie CROSS JOIN jsonb_populate_record(null::meta.enrichment, ie.rule) j
    ON CONFLICT (enrichment_id) DO UPDATE
    SET parent_enrichment_id = EXCLUDED.parent_enrichment_id, 
    source_id = EXCLUDED.source_id, 
    name = EXCLUDED.name, 
    description = EXCLUDED.description, 
    cast_datatype = EXCLUDED.cast_datatype, 
    attribute_name = EXCLUDED.attribute_name, 
    expression = EXCLUDED.expression, 
    rule_type_code = EXCLUDED.rule_type_code, 
    validation_action_code = EXCLUDED.validation_action_code, 
    validation_type_code = EXCLUDED.validation_type_code, 
    keep_current_flag = EXCLUDED.keep_current_flag, 
    window_function_flag = EXCLUDED.window_function_flag,
    unique_flag = EXCLUDED.unique_flag, 
    active_flag = EXCLUDED.active_flag, 
    update_datetime = in_imp.create_datetime, 
    updated_userid = 'Import ' || in_imp.import_id;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, format('Upserted %s enrichments', v_count),'svc_import_execute', 'I', clock_timestamp());

END;
$function$;CREATE OR REPLACE FUNCTION meta.impc_upsert_output(in_o meta.import_object, in_imp meta.import)
    RETURNS jsonb
    LANGUAGE plpgsql
AS
$function$
DECLARE
	v_id int := in_o.id;
	v_error text;	
	v_body jsonb;
BEGIN

v_body := in_o.body || 
	jsonb_build_object('output_package_parameters',jsonb_build_object('table_name', COALESCE(in_o.body->>'table_name',in_o.name),
																	  'table_schema', COALESCE(in_o.body->>'schema_name','')) ) ;

IF v_body->'error' IS NOT NULL THEN
	RETURN v_body;
END IF;

IF v_id IS NULL THEN
	INSERT INTO meta.output(output_type ,output_name ,active_flag ,created_userid ,create_datetime ,output_package_parameters ,retention_parameters ,output_description ,output_sub_type,
	 alert_parameters ,post_output_type , project_id)
	SELECT COALESCE(j.output_type,'table'), j.output_name, COALESCE(j.active_flag, true), 'Import ' || in_imp.import_id, in_imp.create_datetime, 
	j.output_package_parameters, j.retention_parameters, j.output_description, j.output_sub_type, 
	j.alert_parameters, COALESCE(j.post_output_type,'none'), in_imp.project_id
	FROM jsonb_populate_record(null::meta.output, v_body ) j
	RETURNING output_id INTO v_id;
ELSE

	SELECT meta.u_append_object(to_jsonb(o),v_body) 
	INTO v_body
	FROM meta.output o
	WHERE o.output_id = v_id;

	UPDATE meta.output t
	SET	output_type = COALESCE(j.output_type,'table'),
		output_name = j.output_name,
		active_flag = COALESCE(j.active_flag, true),
		update_datetime = in_imp.create_datetime,
		output_package_parameters = j.output_package_parameters,
		updated_userid = 'Import ' || in_imp.import_id,
		retention_parameters = j.retention_parameters,
		output_description = j.output_description,
		output_sub_type = j.output_sub_type,
		alert_parameters = j.alert_parameters,
		post_output_type = COALESCE(j.post_output_type,'none')
	FROM jsonb_populate_record(null::meta.output, v_body) j 
	WHERE t.output_id = v_id;

	--delete/insert columns TODO: compare columns & only update different

		DELETE FROM meta.output_source_column osc
		USING meta.output_source os
		WHERE os.output_id = v_id AND os.output_source_id = osc.output_source_id;
		
		DELETE FROM meta.output_column oc
		WHERE oc.output_id = v_id;

		DELETE FROM meta.output_source os
		WHERE os.output_id = v_id;

END IF;


-- parse & insert columns
INSERT INTO meta.output_column(output_id ,position ,name ,datatype  ,created_userid ,create_datetime)
SELECT v_id, el.idx, col[1] name, COALESCE(col[2],'string') datatype,  'Import ' || in_imp.import_id, in_imp.create_datetime
FROM jsonb_array_elements_text(in_o.body->'columns') WITH ORDINALITY  el(js, idx)
CROSS JOIN regexp_split_to_array(js, '\s+') col;

-- TODO: add datatype validation when we add create table DDL

SELECT meta.u_validate_output(o) INTO v_error
FROM meta.output o
WHERE o.output_id = v_id;

IF v_error != '' THEN
	RETURN jsonb_build_object('output_name',in_o.name,'error', v_error);
END IF;

RETURN null;
END;
$function$;CREATE OR REPLACE FUNCTION meta.impc_upsert_raw_attributes(in_imp meta.import)
    RETURNS jsonb
    LANGUAGE plpgsql
AS
$function$
DECLARE
	v_count int;
	v_err jsonb;
BEGIN

    -- raw attributes temp table
    CREATE TEMP TABLE _imp_raw_attribute ON COMMIT DROP AS
	WITH ct AS (
    	SELECT io.id source_id, io.body->>'source_name' source_name, meta.impc_parse_raw_attribute(ra) raw_parsed
		FROM meta.import_object io 
		CROSS JOIN jsonb_array_elements(io.body->'raw_attributes') ra
		WHERE io.import_id = in_imp.import_id 
        AND io.object_type = 'source'
	)
	SELECT ra.raw_attribute_id, ct.source_id, ct.source_name, raw_parsed
    FROM ct
	LEFT JOIN meta.raw_attribute ra ON ra.source_id = ct.source_id AND ra.column_alias = raw_parsed->>'name';

	-- check parse errors
	SELECT jsonb_agg(jsonb_build_object('source_name', source_name, 'error', raw_parsed->'error'))
	INTO v_err
	FROM _imp_raw_attribute r 
	WHERE raw_parsed->>'error' IS NOT NULL;

	IF v_err IS NOT NULL THEN
		RETURN v_err;
	END IF;

	UPDATE _imp_raw_attribute SET raw_attribute_id = nextval('meta.seq_raw_attribute'::regclass)::int
	WHERE raw_attribute_id IS NULL;

	--upsert raw attributes
	INSERT INTO meta.raw_attribute(raw_attribute_id, source_id ,raw_attribute_name ,column_normalized ,data_type, datatype_schema, version_number ,
	column_alias ,unique_flag ,update_datetime ,updated_userid)
	SELECT j.raw_attribute_id, j.source_id, j.raw_parsed->>'name',  
	j.raw_parsed->>'name', j.raw_parsed->>'data_type',j.raw_parsed->'datatype_schema'
	,1 ,j.raw_parsed->>'name', false, in_imp.create_datetime, 'Import ' || in_imp.import_id
	FROM _imp_raw_attribute j
    ON CONFLICT (raw_attribute_id)
     DO UPDATE SET 
		raw_attribute_name = EXCLUDED.raw_attribute_name,
		column_normalized = EXCLUDED.column_normalized,
		data_type = EXCLUDED.data_type,
		datatype_schema = EXCLUDED.datatype_schema,
		update_datetime = in_imp.create_datetime,
		updated_userid = 'Import ' || in_imp.import_id;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, format('Upserted %s raw attributes', v_count),'imp_upsert_raw_attributes_core', 'I', clock_timestamp());

	
	-- delete raw attributes not in the import file
	DELETE FROM meta.raw_attribute ra
	WHERE ra.raw_attribute_id IN (SELECT r.raw_attribute_id 
			FROM meta.raw_attribute r 
			JOIN meta.source s ON s.source_id = r.source_id AND s.project_id = in_imp.project_id
			LEFT JOIN _imp_raw_attribute ir ON ir.raw_attribute_id = r.raw_attribute_id
			WHERE ir.raw_attribute_id IS NULL
			);

	GET DIAGNOSTICS v_count = ROW_COUNT;
	INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
	VALUES ( in_imp.log_id, format('Deleted %s raw attributes', v_count),'imp_upsert_raw_attributes_core', 'I', clock_timestamp());

	RETURN null;

END;
$function$;CREATE OR REPLACE FUNCTION meta.impc_upsert_relations(in_imp meta.import)
    RETURNS jsonb
    LANGUAGE plpgsql
AS
$function$
DECLARE 
    v_err jsonb;
    v_count int;
BEGIN
    -- create relation temp table
    CREATE TEMP TABLE _imp_relation ON COMMIT DROP AS
    WITH ir AS (
        SELECT j.relation_name, j.source_id, j.related_source_id, j.expression, j.source_cardinality, j.related_source_cardinality, j.expression_parsed, 
        COALESCE(j.active_flag, true) active_flag, j.description, COALESCE(j.primary_flag, true) primary_flag, el rel, rel_js->'error' error
        FROM meta.import_object o 
        CROSS JOIN jsonb_array_elements(o.body) el
        CROSS JOIN meta.imp_decode_relation(el,in_imp.project_id) rel_js
        CROSS JOIN jsonb_populate_record(null::meta.source_relation, rel_js) j
        WHERE o.object_type = 'relations' AND o.import_id = in_imp.import_id
    )
     SELECT sr.source_relation_id, ir.*, null::jsonb rel_parsed
     FROM ir LEFT JOIN meta.source_relation sr ON ir.source_id = sr.source_id AND ir.related_source_id = sr.related_source_id AND ir.relation_name = sr.relation_name;


	-- check decode errors
    SELECT jsonb_agg(jsonb_build_object('name', r.rel->>'name', 'error', r.error))
    INTO v_err
    FROM _imp_relation r
    WHERE r.error IS NOT NULL;

	IF v_err IS NOT NULL THEN
        RETURN jsonb_build_object('error','Relation error(s)','error_detail',v_err);
    END IF;

	-- check duplicates
    WITH dup AS (
        SELECT r.source_id, r.relation_name, r.related_source_id 
		FROM _imp_relation r 
        GROUP BY r.source_id, r.relation_name, r.related_source_id HAVING COUNT(1) > 1   
        )
    SELECT jsonb_agg(jsonb_build_object('name', format('[%s]-%s-[%s]',s.source_name, r.relation_name, rs.source_name)))
    INTO v_err
    FROM dup r 
    LEFT JOIN meta.source s ON s.source_id = r.source_id
    LEFT JOIN meta.source rs ON s.source_id = r.related_source_id;

	IF v_err IS NOT NULL THEN
        RETURN jsonb_build_object('error','Duplicate relations','error_detail',v_err);
    END IF;

    -- delete relations not present in import file
    DELETE FROM meta.source_relation sr
    WHERE sr.source_id IN (SELECT source_id FROM meta.source s WHERE s.project_id = in_imp.project_id)
    AND sr.source_relation_id NOT IN (SELECT source_relation_id FROM _imp_relation);
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, format('Deleted %s relations', v_count),'svc_import_execute', 'I', clock_timestamp());

    UPDATE _imp_relation SET source_relation_id = nextval('meta.seq_source_relation'::regclass)::int WHERE source_relation_id IS NULL;

    -- insert new relations
    INSERT INTO meta.source_relation(source_relation_id, relation_name ,source_id ,related_source_id ,expression ,source_cardinality ,related_source_cardinality ,expression_parsed ,active_flag ,create_datetime ,created_userid ,description ,primary_flag)
    SELECT source_relation_id, relation_name ,source_id ,related_source_id ,expression ,source_cardinality ,related_source_cardinality ,expression_parsed ,active_flag ,in_imp.create_datetime, 'Import ' || in_imp.import_id created_userid, description ,primary_flag 
    FROM _imp_relation
    ON CONFLICT (source_relation_id) DO UPDATE
    SET relation_name = EXCLUDED.relation_name, 
        source_id = EXCLUDED.source_id, 
        related_source_id = EXCLUDED.related_source_id ,
        expression = EXCLUDED.expression, 
        source_cardinality = EXCLUDED.source_cardinality ,
        related_source_cardinality = EXCLUDED.related_source_cardinality, 
        active_flag = EXCLUDED.active_flag, 
        update_datetime = in_imp.create_datetime, 
        updated_userid = 'Import ' || in_imp.import_id, 
        description = EXCLUDED.description, 
        primary_flag = EXCLUDED.primary_flag;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, format('Upserted %s relations', v_count),'svc_import_execute', 'I', clock_timestamp());

    -- parse expressions
    UPDATE _imp_relation ir
    SET rel_parsed = meta.impc_parse_relation(to_jsonb(ir));

	-- check parse errors
    SELECT jsonb_agg(jsonb_build_object('name', r.rel->>'name', 'expression', r.expression, 'error', rel_parsed->>'error'))
    INTO v_err
    FROM _imp_relation r
    WHERE rel_parsed->>'error' IS NOT NULL OR rel_parsed->>'expression_parsed' IS NULL;

	IF v_err IS NOT NULL THEN
        RETURN jsonb_build_object('error','Relation expression parse error(s)','error_detail',v_err);
    END IF;

    UPDATE meta.source_relation sr
    SET expression_parsed = ir.rel_parsed->>'expression_parsed'
    FROM _imp_relation ir 
    WHERE ir.source_relation_id = sr.source_relation_id;

    -- save relations for testing
    DELETE FROM meta.relation_test WHERE project_id = in_imp.project_id;

    INSERT INTO meta.relation_test (source_relation_id, project_id, expression)
    SELECT ir.source_relation_id, in_imp.project_id, ir.rel_parsed->>'test_expression'
    FROM _imp_relation ir;
     
    DROP TABLE _imp_relation;

    RETURN null;
END;
$function$;CREATE OR REPLACE FUNCTION meta.impc_upsert_source(in_o meta.import_object, in_imp meta.import)
    RETURNS jsonb
    LANGUAGE plpgsql
AS
$function$
DECLARE
	v_id int := in_o.id;
	v_hub_view_name text;	
	v_body jsonb;
BEGIN

v_hub_view_name :=  COALESCE(v_body->>'target_table_name',in_o.name); 
v_body := in_o.body || jsonb_build_object('ingestion_parameters',jsonb_build_object('source_query', in_o.body->>'source_query'));

IF v_body->'error' IS NOT NULL THEN
	RETURN v_body;
END IF;

IF in_o.id IS NULL THEN
	INSERT INTO meta.source(source_name ,source_description ,active_flag , ingestion_parameters, create_datetime, created_userid, parsing_parameters ,cdc_refresh_parameters ,alert_parameters ,file_type ,refresh_type ,
	connection_type ,initiation_type ,cost_parameters ,parser ,	hub_view_name , project_id)
	SELECT j.source_name, COALESCE(v_body->>'description',''), COALESCE(j.active_flag,true), j.ingestion_parameters, in_imp.create_datetime, 'Import ' || in_imp.import_id created_userid, j.parsing_parameters, j.cdc_refresh_parameters, j.alert_parameters, j.file_type, COALESCE(j.refresh_type,'full'), 
	j.connection_type, j.initiation_type, j.cost_parameters, j.parser, v_hub_view_name, in_imp.project_id
	FROM jsonb_populate_record(null::meta.source, v_body) j
	RETURNING source_id INTO v_id;

ELSE
	-- Get current source json
	-- Update all attributes to values in import file, then append all default values
	-- Do not update existing attributes not present in import

	SELECT meta.u_append_object(to_jsonb(s),v_body) 
	INTO v_body
	FROM meta.source s
	WHERE s.source_id = in_o.id;

	UPDATE meta.source t
	SET	source_name = j.source_name, 
		source_description = COALESCE(j.source_description,''),
		active_flag = COALESCE(j.active_flag, true),
		ingestion_parameters = j.ingestion_parameters,
		update_datetime = in_imp.create_datetime,
		parsing_parameters = j.parsing_parameters,
		cdc_refresh_parameters = j.cdc_refresh_parameters,
		updated_userid = 'Import ' || in_imp.import_id,
		alert_parameters = j.alert_parameters,
		file_type = j.file_type,
		refresh_type = j.refresh_type,
		connection_type = j.connection_type,
		initiation_type = j.initiation_type,
		cost_parameters = j.cost_parameters,
		parser = j.parser,
		hub_view_name = v_hub_view_name
	FROM jsonb_populate_record(null::meta.source,  v_body) j 
	WHERE t.source_id = in_o.id;
END IF;
			  
RETURN null;	
END;
$function$;CREATE OR REPLACE FUNCTION meta.svc_check_attribute_name(in_source_id int, in_name text)
    RETURNS json
    LANGUAGE 'plpgsql'

    COST 10
    VOLATILE 
AS $BODY$

DECLARE 
v_ret json;
BEGIN

 		WITH ct AS(
		SELECT 'raw' as attribute_type, raw_attribute_id as id
        FROM meta.raw_attribute r
        WHERE r.source_id = in_source_id AND r.column_alias = in_name
        UNION ALL
        SELECT 'system', system_attribute_id
        FROM meta.system_attribute s
        WHERE s.table_type @> ARRAY['hub'] AND s.name = in_name
        UNION ALL
        SELECT 'enrichment', enrichment_id
        FROM meta.enrichment e 
        WHERE e.source_id = in_source_id AND e.active_flag AND e.attribute_name = in_name
		 )
		SELECT row_to_json(ct)
		 INTO v_ret
		 FROM ct; 

	RETURN  v_ret;
		
END;

$BODY$;CREATE OR REPLACE FUNCTION meta.svc_import_complete(in_import_id int, in_status_code char(1), in_err text = null)
    RETURNS void
    LANGUAGE plpgsql
AS
$function$

DECLARE
    v_import_id int;
    v_log_id int;
BEGIN
    UPDATE meta.import SET status_code = in_status_code
    WHERE import_id = in_import_id
    RETURNING log_id INTO v_log_id;

    IF in_status_code = 'F' THEN
       INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( v_log_id, 'Import failed : ' || COALESCE(in_err, 'NULL'),'svc_import_complete', 'E', clock_timestamp());
    ELSEIF in_status_code = 'Q' THEN
        INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( v_log_id, 'Import loaded. ' || COALESCE(in_err, ''),'svc_import_complete', 'I', clock_timestamp());
    ELSEIF in_status_code = 'D' THEN
        INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( v_log_id, in_err || ' Click Restart to proceed with import','svc_import_complete', 'W', clock_timestamp() + interval '1 second'); -- makes it appear last
    ELSE 
       INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( v_log_id, 'Import loaded. ' || COALESCE(in_err, ''),'svc_import_complete', 'I', clock_timestamp());
    END IF;        
END;
$function$;
CREATE OR REPLACE FUNCTION meta.svc_import_execute(in_import_id int, in_force_flag boolean = false)
    RETURNS boolean
    LANGUAGE plpgsql
AS
$function$
DECLARE
    v_imp meta.import;
    v_err jsonb;
    v_test_flag boolean = false;
BEGIN
    SELECT * INTO v_imp FROM meta.import WHERE import_id = in_import_id;

    IF v_imp.format IS NULL THEN
        PERFORM meta.svc_import_complete(in_import_id, 'F', 'Blank format or missing meta.yaml');
        RETURN false;
    END IF;

    v_err := meta.impc_execute(v_imp);
    IF v_err IS NOT NULL THEN
        PERFORM meta.svc_import_complete(in_import_id, 'F', v_err::text);
        RETURN false;
    END IF;
    v_test_flag := true;

    RETURN true;
    END;
$function$;
CREATE OR REPLACE FUNCTION meta.svc_import_get_log(in_import_id int)
    RETURNS text
    LANGUAGE plpgsql
AS
$function$

BEGIN
    RETURN (
        SELECT string_agg(ln, E'\n')
            FROM ( SELECT format(E'%s\t%s\t%s', l.insert_datetime, CASE l.severity WHEN 'I' THEN 'INFO' WHEN 'W' THEN 'WARN' WHEN 'E' THEN 'ERROR' END, l.message) ln
                FROM log.actor_log l
                JOIN meta.import i ON i.log_id = l.log_id AND i.import_id = in_import_id
                ORDER BY l.insert_datetime
            ) t
    );

END;
$function$;
CREATE OR REPLACE FUNCTION meta.svc_import_load_object(in_import_id int, in_path text, in_body text)
    RETURNS void
    LANGUAGE plpgsql
AS
$function$

DECLARE
    v_object_type text;
    v_format text;
BEGIN
    -- check format
    IF in_path = 'meta.yaml' THEN
        PERFORM meta.imp_check_format(in_import_id, (in_body::json)->>'format');
    ELSEIF in_path = 'variables.yaml' THEN
        INSERT INTO meta.import_object(  import_id, file_path, object_type, body_text) VALUES
        (in_import_id, in_path, 'variables', in_body);
    ELSEIF in_path = 'relations.yaml' THEN
        INSERT INTO meta.import_object(  import_id, file_path, object_type, body_text) VALUES
        (in_import_id, in_path, 'relations', in_body);
    ELSEIF in_path = 'defaults.yaml' THEN
        INSERT INTO meta.import_object(  import_id, file_path, object_type, body_text) VALUES
        (in_import_id, in_path, 'defaults', in_body);
    ELSE 
        v_object_type := substring(in_path from '^(\w+)s/');

        IF v_object_type IS NULL THEN
            RAISE EXCEPTION 'Unable to parse import object type, invalid path %', in_path;
        END IF;

        IF v_object_type NOT IN ('source','output', 'group', 'token','output_template','source_template','rule_template','relation_template') THEN
                RAISE EXCEPTION 'Unknown object type % in file %', v_object_type, in_path;
        END IF;

        INSERT INTO meta.import_object(  import_id, file_path, object_type, body_text) VALUES
        (in_import_id, in_path, v_object_type, in_body);
    END IF;

END;
$function$;
CREATE OR REPLACE FUNCTION meta.svc_import_start()
    RETURNS int
    LANGUAGE plpgsql
AS
$function$

DECLARE
    v_import_id int;
    v_project_id int;
BEGIN
    v_project_id := (SELECT project_id FROM meta.project WHERE default_flag);

        INSERT INTO meta.import(  project_id, type, status_code, created_userid ) VALUES
        ( v_project_id, 'import', 'I', 'system')
        RETURNING import_id INTO v_import_id;

        RETURN v_import_id;
END;
$function$;
CREATE OR REPLACE FUNCTION meta.svc_parse_enrichment(in_parameters JSON, in_template_check_flag boolean DEFAULT FALSE, in_mode text = 'ui')
    RETURNS JSON
    LANGUAGE 'plpgsql'

AS
$BODY$

DECLARE
    in_enr                       meta.enrichment;
    v_ret_expression             TEXT    := ''; -- expression with  attributes replaced with datatypes
    v_expression_parsed          TEXT    := ''; -- parsed expression
    v_attribute_name             TEXT;
    v_source_name                TEXT;
    v_parameter_source_id        INT;
    v_attribute_name_error       TEXT;
    v_enrichment_name_error      Text;
    v_attribute_check_json       JSON;
    v_expression_position        INT     := 0;
    v_parameter_position         INT     := 0;
    v_in_square_brackets_flag    BOOLEAN := FALSE;
    v_in_quotes_flag             BOOLEAN := FALSE;
    v_expression_length          INT;
    v_in_attribute_flag          BOOLEAN;
    v_field_start_position       INT;
    v_char                       CHAR;
    v_next_char                  CHAR;
    v_window_function_flag       BOOLEAN;
    v_attribute_start_position   INT;
    v_template_match_flag        BOOLEAN := FALSE;
    v_ret_params                 json;
    v_ret_aggs                   json;
    v_agg_error                  text;
    v_aggregate_id               int;
    v_project_id                 int;
    v_parameter                  parameter_map;
    v_saved_parameter_position   int;
    v_next_relation_path         json;
    v_enrichment_parameter_id    int;
    v_source_relation_ids        int[];
    v_start                     int;
    v_agg_start                 int;
    v_end                       int;
    v_last_end                  int := 1;
    v_error_ids                 int[];
    v_relation_path_count       int;

BEGIN

SELECT * FROM json_populate_record(null::meta.enrichment, in_parameters -> 'enrichment' )  INTO in_enr;

IF in_enr.expression IS NULL THEN
    RETURN  json_build_object('error', 'Expression is NULL', 'expression', NULL);
END IF;

v_project_id := meta.u_get_source_project(in_enr.source_id);

IF (in_enr.expression) LIKE '%/*%'
THEN
    RETURN json_build_object('error', 'Please use the description section for any comments.', 'expression', NULL);
END IF;

-- check attribute name syntax
IF NOT in_enr.attribute_name ~ '^[a-z_]+[a-z0-9_]*$'
THEN
    v_attribute_name_error := 'Invalid attribute name syntax. Attribute name has to start with lowercase letter or _ It may contain lowercase letters, numbers and _';
END IF;

v_template_match_flag := in_template_check_flag and exists (select 1 from meta.enrichment WHERE source_id = in_enr.source_id AND expression = in_enr.expression AND attribute_name = in_enr.attribute_name AND name = in_enr.name);

-- check attribute name uniquiness
v_attribute_check_json := meta.svc_check_attribute_name(in_enr.source_id, in_enr.attribute_name);
IF v_attribute_check_json ->> 'attribute_type' IN ('raw', 'system') OR (
        v_attribute_check_json ->> 'attribute_type' = 'enrichment' AND (v_attribute_check_json ->> 'id')::int IS DISTINCT FROM in_enr.enrichment_id AND NOT v_template_match_flag)
THEN
    v_attribute_name_error := 'Invalid attribute name: ' || (v_attribute_check_json ->> 'attribute_type')
        || ' attribute with this name already exists. ' || CASE WHEN in_template_check_flag THEN 'Name, Attribute Name, Source_id, and Expression must match EXACTLY to apply a template to an existing rule.' ELSE '' END;
END IF;
-- check enrichment name uniqueness
v_enrichment_name_error := CASE WHEN EXISTS(SELECT 1
                                    FROM meta.enrichment e
                                    WHERE e.name = in_enr.name AND e.source_id = in_enr.source_id AND e.active_flag AND
                                        e.enrichment_id IS DISTINCT FROM in_enr.enrichment_id AND NOT v_template_match_flag) THEN 'Duplicate enrichment name. ' || CASE WHEN in_template_check_flag THEN 'Name, Attribute Name, Source_id, and Expression must match EXACTLY to apply a template to an existing rule.' ELSE '' END 
                                        END;

IF v_enrichment_name_error IS NOT NULL OR v_attribute_name_error IS NOT NULL
THEN
    RETURN json_build_object('error', COALESCE(v_attribute_name_error,'') || COALESCE(v_enrichment_name_error,''));
END IF;

in_enr.window_function_flag := in_enr.expression ~* 'over\s*\(.*\)';

IF NOT in_enr.keep_current_flag AND in_enr.window_function_flag THEN
    RETURN json_build_object('error', 'When expression contains window function, enrichment is required to use keep current recalculation mode');
END IF;


PERFORM meta.u_read_enrichment_parameters(in_parameters -> 'params');

IF NOT EXISTS(SELECT 1 FROM _params) AND in_enr.enrichment_id IS NOT NULL THEN
    -- enrichment is saved, but no parameters passed: read from enrichment_parameters
    INSERT INTO _params
    SELECT ep.enrichment_parameter_id, 
    ep.parent_enrichment_id,
    ep.type,
    ep.enrichment_id,
    ep.raw_attribute_id,
    ep.system_attribute_id,
    ep.source_id,
    ep.source_relation_ids,
    ep.self_relation_container,
    ep.create_datetime,
    ep.aggregation_id,
    CASE WHEN ep.source_id <> in_enr.source_id THEN s.source_name -- Use 'This' when parameter is from current source and is not using relation
    WHEN COALESCE(ep.source_relation_ids,'{}'::int[]) = '{}' THEN 'This' ELSE s.source_name END,  
    meta.u_enr_query_get_enrichment_parameter_name(ep) attribute_name,
    CASE WHEN  COALESCE(ep.source_relation_ids,'{}'::int[]) <> '{}'::int[] THEN meta.u_get_next_relation_path(in_enr.source_id, ep.source_id, CASE WHEN ep.aggregation_id IS NULL THEN '1' ELSE 'M' END, ep.source_relation_ids) END paths,
    null::int p_start, null::int p_end, null, null
    FROM meta.enrichment_parameter ep
    LEFT JOIN meta.source s ON ep.source_id = s.source_id
    WHERE parent_enrichment_id = in_enr.enrichment_id;
END IF;


-- parse aggregates
v_agg_error := meta.u_parse_enrichment_aggregates(in_enr.expression);
IF v_agg_error IS DISTINCT FROM '' THEN
    RETURN json_build_object('error', v_agg_error);
END IF;

RAISE DEBUG 'Parsed % aggregates',(SELECT COUNT(1) FROM  _aggs_parsed);

    v_expression_length = length(in_enr.expression);

    WHILE v_expression_position <= v_expression_length LOOP
        v_expression_position := v_expression_position + 1;
        v_char = substring(in_enr.expression, v_expression_position, 1);

        IF (v_in_quotes_flag)
        THEN
            IF v_char = ''''
            THEN
                v_next_char := substring(in_enr.expression, v_expression_position + 1, 1);
                IF v_next_char = ''''
                THEN
                    --Double quote escape character. Keep going
                    v_expression_position := v_expression_position + 1;
                ELSE
                    v_in_quotes_flag := FALSE;
                END IF;
            ELSE
                --Still in a quoted string, keep going down the string
            END IF;

        ELSEIF v_in_square_brackets_flag
            THEN
                --Check for error
                IF v_char = '['
                THEN
                    RETURN json_build_object('error', 'Nested [ brackets detected. Found ' || v_char || ' at position ' || v_expression_position || '.');
                --Check to see square bracket is ending
                ELSEIF v_char = ']'
                THEN
                    -- check empty brackets
                    IF v_expression_position - v_field_start_position = 1 THEN 
                        RETURN json_build_object('error', 'Empty brackets at position ' || v_expression_position);
                    END IF;

                    v_source_name = substring(in_enr.expression, v_field_start_position + 1, v_expression_position - v_field_start_position - 1);

                    IF v_source_name ~ '^[0-9]+$' THEN -- numbers only, ignore as array index
                        v_in_square_brackets_flag := false;
                        CONTINUE;
                    END IF;    

                    --Check next char is valid
                    v_next_char = substring(in_enr.expression, v_expression_position + 1, 1);
                    IF v_next_char <> '.' THEN
                        RETURN json_build_object('error', 'Improper expression detected. Found ' || v_char || ' at position ' || v_expression_position || '. Expected .');
                    ELSE
                        --If its kosher, we are out of the square brackets. Check source name

                    v_parameter_source_id := null;
                    IF v_source_name = 'This' THEN
                        v_parameter_source_id = in_enr.source_id;
                    ELSE
                        SELECT s.source_id INTO v_parameter_source_id
                        FROM meta.source s
                        WHERE s.project_id = v_project_id AND s.source_name = v_source_name;
                    END IF;

                    IF v_parameter_source_id IS NULL THEN
                        RETURN json_build_object('error', 'Source name ' || v_source_name || ' does not exist in project_id=' || v_project_id || ' position ' || v_expression_position);
                    END IF;
                        -- source validated, move on to attribute
                        v_in_square_brackets_flag := FALSE;
                        v_expression_position := v_expression_position + 2;
                        v_attribute_start_position := v_expression_position;
                        v_in_attribute_flag = true;
                    END IF;
                ELSE
                    --We aren't out of the square brackets yet. Keep going.
                END IF;
        ELSEIF v_in_attribute_flag
            THEN
                IF NOT v_char ~ '\w' OR v_expression_position > v_expression_length 
                THEN
                    --End of field, send if off
                    v_in_attribute_flag = FALSE;

                    v_attribute_name := substring(in_enr.expression, v_attribute_start_position, v_expression_position - v_attribute_start_position);
                   
                    -- check self-reference
                    IF v_attribute_name = in_enr.attribute_name AND v_parameter_source_id = in_enr.source_id THEN
                        RETURN json_build_object('error', 'Self-referencing attribute detected in postition ' || v_attribute_start_position);
                    END IF;
                    -- lookup parameter attribute
                    v_parameter := meta.u_lookup_source_attribute(v_parameter_source_id, v_attribute_name);
                    IF v_parameter.error IS NOT NULL THEN
                        RETURN json_build_object('error', v_parameter.error);
                    END IF;

                    IF v_parameter.enrichment_id = in_enr.enrichment_id THEN
                        RETURN json_build_object('error', format('Self-reference detected in attribute [%s].%s at position %s',v_source_name, v_attribute_name, v_attribute_start_position));
                    END IF;

                    SELECT id INTO v_aggregate_id
                    FROM _aggs_parsed
                    WHERE v_field_start_position BETWEEN a_start AND a_end 
                        AND v_expression_position - 1 BETWEEN a_start AND a_end;

                    IF v_source_name = 'This' AND v_aggregate_id IS NOT NULL THEN
                        RETURN json_build_object('error', format('Aggregate not allowed on [This] source attribute %s at position %s', v_attribute_name, v_attribute_start_position));
                    END IF;

                    v_parameter_position := v_parameter_position + 1;

                    -- we have parameter at v_parameter_position, let's compare it with what is currently saved 
                    SELECT id, 
                    CASE WHEN meta.u_compare_nulls(aggregation_id, v_aggregate_id) THEN source_relation_ids ELSE '{}'::int[] END -- reset saved path when was aggregation  
                    INTO v_enrichment_parameter_id, v_source_relation_ids 
                    FROM _params p
                    WHERE p.source_name = v_source_name AND p.attribute_name = v_attribute_name AND p.id = v_parameter_position;

                    IF v_enrichment_parameter_id IS NOT NULL THEN
                        -- saved or passed parameter exists in the same position, update aggregation_id & path if changed,continue
                        RAISE DEBUG 'v_attribute_name % exists in same position',v_attribute_name;
                        IF v_source_name <> 'This' THEN-- update paths and positions
                            v_next_relation_path := meta.u_get_next_relation_path(in_enr.source_id, v_parameter_source_id, CASE WHEN v_aggregate_id IS NULL THEN '1' ELSE 'M' END,v_source_relation_ids);
                            UPDATE _params SET paths = v_next_relation_path, source_relation_ids = meta.u_json_array_to_int_array(v_next_relation_path->'relation_ids'),
                            p_start = v_field_start_position, p_end = v_expression_position - 1,
                            aggregation_id = v_aggregate_id,
                            datatype = v_parameter.datatype,
                            datatype_schema = v_parameter.datatype_schema
                            WHERE id = v_enrichment_parameter_id;
                        ELSE -- update positions
                            UPDATE _params SET 
                            p_start = v_field_start_position, p_end = v_expression_position - 1,
                            datatype = v_parameter.datatype,
                            datatype_schema = v_parameter.datatype_schema
                            WHERE id = v_enrichment_parameter_id;
                        END IF;
                        CONTINUE;
                    END IF;

                    
                    -- search parameter with same name and aggregation
                    IF NOT EXISTS(SELECT 1 FROM _params p
                        WHERE p.source_name = v_source_name AND p.attribute_name = v_attribute_name AND meta.u_compare_nulls(aggregation_id, v_aggregate_id)
                        ) THEN
                        -- parameter doesn't exist: insert it and slide positions of all following parameters
                        RAISE DEBUG 'v_attribute_name % doesn''t exists, inserting into position %',v_attribute_name,v_parameter_position;
                        UPDATE _params SET id = id + 1
                        WHERE  id >= v_parameter_position;

                        v_source_relation_ids := meta.u_get_relation_path_core(in_enr.enrichment_id, v_source_name);

                        -- get relation paths starting from blank chain
                        v_next_relation_path := CASE WHEN v_source_name <> 'This' 
                            THEN meta.u_get_next_relation_path(in_enr.source_id, v_parameter_source_id, 
                            CASE WHEN v_aggregate_id IS NULL THEN '1' ELSE 'M' END,
                            v_source_relation_ids -- get path from core import parameter temp table if it exists, otherwise null
                            ) 
                        END;

                        IF  in_mode = 'import' AND v_source_relation_ids IS NULL THEN
                                                -- check for multiple paths

                            IF EXISTS(SELECT 1 FROM json_array_elements(v_next_relation_path->'path') h
                                        WHERE json_array_length(h->'selections') > 1)
                                THEN 
                                    RETURN json_build_object('error', format('Multiple relation paths exist for source %s. Specify desired path in rule parameters.',v_source_name));
                            END IF;			
                        END IF;			

                        INSERT INTO _params (id, parent_enrichment_id, 
                            type, enrichment_id, raw_attribute_id, system_attribute_id, source_id, 
                            source_relation_ids, 
                            self_relation_container, 
                            aggregation_id,
                            source_name,
                            attribute_name ,
                            paths ,
                            p_start ,
                            p_end,
                            datatype,
                            datatype_schema 
                            )
                        VALUES (
                            v_parameter_position, in_enr.enrichment_id,
                            v_parameter.type, v_parameter.enrichment_id, v_parameter.raw_attribute_id , v_parameter.system_attribute_id ,v_parameter_source_id,
                            meta.u_json_array_to_int_array(v_next_relation_path->'relation_ids'),
                            CASE WHEN v_source_name <> 'This' AND in_enr.source_id = v_parameter_source_id THEN 'Related' END, -- TODO: this can be simplified into a flag
                            v_aggregate_id,
                            v_source_name,
                            v_attribute_name,
                            v_next_relation_path,
                            v_field_start_position,
                            v_expression_position - 1,
                            v_parameter.datatype,
                            v_parameter.datatype_schema
                        );
                        CONTINUE;
                    END IF;

                    -- search parameter later in expression with the same aggregation
                    SELECT MIN(id) INTO v_saved_parameter_position
                    FROM _params p
                    WHERE p.source_name = v_source_name AND p.attribute_name = v_attribute_name 
                    AND meta.u_compare_nulls(aggregation_id, v_aggregate_id)
                    AND id > v_parameter_position;
                    
                    IF v_saved_parameter_position IS NOT NULL THEN
                        -- parameter exists in later/greater position. This means that saved/passed parameter in current position was either removed from expression or order of parameters in the expression has changed 
                        -- swap positions of saved and current parameters

                        RAISE DEBUG 'v_attribute_name % exists later at position %',v_attribute_name,v_saved_parameter_position;
                        UPDATE _params SET id = -1
                        WHERE  id = v_parameter_position;
                        
                        UPDATE _params SET id = v_parameter_position,
                            p_start = v_field_start_position,
                            p_end = v_expression_position - 1,
                            aggregation_id = v_aggregate_id,
                            datatype = v_parameter.datatype,
                            datatype_schema = v_parameter.datatype_schema
                        WHERE  id = v_saved_parameter_position;

                        UPDATE _params SET id = v_saved_parameter_position
                        WHERE  id = -1;


                        CONTINUE;
                    END IF;
                    
                    SELECT MAX(id) INTO v_saved_parameter_position
                    FROM _params p
                    WHERE p.source_name = v_source_name AND p.attribute_name = v_attribute_name AND meta.u_compare_nulls(aggregation_id, v_aggregate_id);


                    IF v_saved_parameter_position IS NOT NULL THEN
                        -- parameter exists in preceding position (duplicate): copy it to current position, including path (if aggregations didn't change)
                        RAISE DEBUG 'v_attribute_name % exists earlier at position %',v_attribute_name,v_saved_parameter_position;
                        -- push parameters down
                        UPDATE _params SET id = id + 1
                        WHERE  id >= v_parameter_position;
                        
                        INSERT INTO _params (id, parent_enrichment_id, 
                            type, enrichment_id, raw_attribute_id, system_attribute_id, source_id, 
                            source_relation_ids, 
                            self_relation_container, 
                            aggregation_id,
                            source_name,
                            attribute_name,
                            paths,
                            p_start,
                            p_end, 
                            datatype,
                            datatype_schema
                            )
                        SELECT v_parameter_position, parent_enrichment_id,
                            type, enrichment_id, raw_attribute_id, system_attribute_id, source_id, 
                            source_relation_ids, 
                            self_relation_container, 
                            v_aggregate_id,
                            source_name,
                            attribute_name,
                            paths,
                            v_field_start_position,
                            v_expression_position - 1,
                            v_parameter.datatype,
                            v_parameter.datatype_schema
                        FROM _params
                        WHERE id = v_saved_parameter_position;
                    ELSE
                        -- this should never happen because we checked all combinations above
                        RETURN json_build_object('error', 'Unable to parse parameter [' || COALESCE(v_source_name,'null') || '].' || COALESCE(v_attribute_name,'null') || ' at position ' || v_field_start_position);
                    END IF;


                ELSE
                    --Still in field keep trucking
                END IF;
        ELSEIF v_char = '['  THEN
                v_in_square_brackets_flag = TRUE;
                v_field_start_position = v_expression_position;
        ELSEIF v_char = '''' THEN
                v_in_quotes_flag = TRUE;
        END IF;

        
    END LOOP;
    -- delete passed/saved parameters after last parsed parameter position
    DELETE FROM _params WHERE id > v_parameter_position;


    -- create test expression
    v_ret_expression := meta.u_build_datatype_test_expr(in_enr.expression);
    
    SELECT json_agg(p) INTO v_ret_params FROM _params p;

    IF v_ret_expression IS NULL AND in_mode = 'ui' THEN
        SELECT json_agg(a) INTO v_ret_aggs FROM _aggs_parsed a;
        RETURN json_build_object('error', format('Unable to parse expression for data type checking. params: %s aggs: %s', v_ret_params, v_ret_aggs));
    END IF;
    -- create parsed expression
    -- replace all aggregate functions in original expression with A<N> pointers
    -- loop through each aggregate and replace parameter expressions with P<N> pointers
    v_last_end = 1;
    FOR v_aggregate_id, v_agg_start, v_start, v_end IN SELECT id, a_function_start, a_start, a_end FROM _aggs_parsed ORDER BY id LOOP
            -- add chunk after last aggregate
            v_expression_parsed := v_expression_parsed || meta.u_parse_expression(in_enr.expression,v_last_end,v_agg_start - 1);
                -- add A<N> pointer
            v_expression_parsed := v_expression_parsed || 'A<' || v_aggregate_id || '>';
            -- check that only single related parameter is used by an aggregation
            SELECT array_agg(id), count(DISTINCT source_relation_ids), max(source_relation_ids)
            INTO v_error_ids, v_relation_path_count, v_source_relation_ids
            FROM _params WHERE aggregation_id = v_aggregate_id AND source_relation_ids IS NOT NULL;

            IF v_relation_path_count > 1 THEN
                RETURN json_build_object('error', format('Multiple parameters %s found in agggregation %s using different relation paths',v_error_ids, v_aggregate_id),
                'params', v_ret_params);
            END IF;

            UPDATE _aggs_parsed a SET expression_parsed = a.function || '(' || meta.u_parse_expression(in_enr.expression,v_start,v_end - 1) || ')',
            relation_ids = v_source_relation_ids
            WHERE id = v_aggregate_id;
        v_last_end = v_end + 1;
    END LOOP;
    -- add expression after last aggregate
    v_expression_parsed := v_expression_parsed || meta.u_parse_expression(in_enr.expression,v_last_end,length(in_enr.expression));
    in_enr.expression_parsed := v_expression_parsed;

    SELECT json_agg(a) INTO v_ret_aggs FROM _aggs_parsed a;

RETURN json_strip_nulls(json_build_object('expression', v_ret_expression, 'enrichment', in_enr, 'params', v_ret_params, 'aggs', v_ret_aggs));

END;

$BODY$;CREATE OR REPLACE FUNCTION meta.svc_select_attribute_types_spark_to_hive()
 RETURNS json
 LANGUAGE plpgsql
AS $function$

BEGIN
RETURN (
   SELECT json_agg(t)
   FROM (
      SELECT unnest(spark_type) spark_type, hive_type, complex_flag 
      FROM meta.attribute_type
   ) t
);

END;
$function$;CREATE OR REPLACE FUNCTION meta.u_build_datatype_test_expr_from_parsed(in_enr meta.enrichment)
    RETURNS text
    LANGUAGE 'plpgsql'

AS
$BODY$

DECLARE
    v_param                     parameter_map;
    v_ep                        meta.enrichment_parameter;
    v_aggregates_exist_flag     boolean = false;
    v_exp_test_select_list      text[] := '{}';
    v_exp_test                  text;
    v_ret_expression            text :=  in_enr.expression_parsed;
    v_agg                       text;
    v_id                        int;

BEGIN

    IF v_ret_expression IS NULL THEN
        RETURN NULL;
    END IF;

    -- replace aggregates
    FOR v_id, v_agg IN SELECT enrichment_aggregation_id, expression FROM meta.enrichment_aggregation WHERE enrichment_id = in_enr.enrichment_id LOOP
        v_ret_expression := replace(v_ret_expression, format('A<%s>', v_id), v_agg);
        v_aggregates_exist_flag = true;
    END LOOP;

    RAISE DEBUG 'v_ret_expression: %',v_ret_expression;
    -- build test expression in this format:
    -- WITH ct AS (SELECT <exp1> p_1, <exp2> p_2 FROM datatypes)
    -- SELECT <expr with P<1> replaced with p_1> as col1 FROM ct

    FOR v_ep IN SELECT *
           FROM meta.enrichment_parameter ep WHERE parent_enrichment_id = in_enr.enrichment_id ORDER BY enrichment_parameter_id LOOP
        
        v_param := meta.u_get_parameter(v_ep);
        RAISE DEBUG 'v_param: %',v_param;
        IF v_param.error IS NOT NULL THEN
            RETURN v_param.error;
        END IF;

        IF v_param.datatype IS NULL THEN
            RETURN NULL;
        END IF;

        v_ret_expression := replace(v_ret_expression, format('P<%s>', v_ep.enrichment_parameter_id), format('p_%s', v_ep.enrichment_parameter_id)) ;

        -- add parameter with datatype
        v_exp_test_select_list := v_exp_test_select_list ||
         (CASE WHEN v_aggregates_exist_flag AND v_ep.aggregation_id IS NULL 
            THEN  'first_value(' || meta.u_datatype_test_expression(v_param.datatype,v_param.datatype_schema) || ')' -- wrap non-aggregated parameter into aggregate for data type testing purposes only
            ELSE  meta.u_datatype_test_expression(v_param.datatype,v_param.datatype_schema) END  || ' p_' || v_ep.enrichment_parameter_id);

    END LOOP;

    RAISE DEBUG 'v_exp_test_select_list: %',v_exp_test_select_list;
    
    IF cardinality(v_exp_test_select_list) > 0 THEN
        v_exp_test := format('WITH ct AS (SELECT %s FROM datatypes) SELECT %s as col1 FROM ct',array_to_string(v_exp_test_select_list,','), v_ret_expression);
    ELSE -- no parameters
        v_exp_test := format('SELECT %s as col1', v_ret_expression);
    END IF;

    RETURN v_exp_test;

END;

$BODY$;



CREATE OR REPLACE FUNCTION meta.u_build_datatype_test_expr_from_parsed(in_sr meta.source_relation)
    RETURNS text
    LANGUAGE 'plpgsql'

AS
$BODY$

DECLARE
    v_param                     parameter_map;
    v_ep                        meta.source_relation_parameter;
    v_exp_test_select_list      text[] := '{}';
    v_exp_test                  text;
    v_ret_expression            text :=  in_sr.expression_parsed;

BEGIN

    IF v_ret_expression IS NULL THEN
        RETURN NULL;
    END IF;

    -- build test expression in this format:
    -- WITH ct AS (SELECT <exp1> p_1, <exp2> p_2 FROM datatypes)
    -- SELECT <expr with P<1> replaced with p_1> as col1 FROM ct

    FOR v_ep IN SELECT *
           FROM meta.source_relation_parameter rp WHERE source_relation_id = in_sr.source_relation_id ORDER BY source_relation_parameter_id LOOP
        
        v_param := meta.u_get_parameter(v_ep);
        RAISE DEBUG 'v_param: %',v_param;
        IF v_param.error IS NOT NULL THEN
            RETURN v_param.error;
        END IF;

        IF v_param.datatype IS NULL THEN
            RETURN NULL;
        END IF;

        v_ret_expression := replace(v_ret_expression, format('P<%s>', v_ep.source_relation_parameter_id), format('p_%s', v_ep.source_relation_parameter_id)) ;

        -- add parameter with datatype
        v_exp_test_select_list := v_exp_test_select_list ||
          ( meta.u_datatype_test_expression(v_param.datatype,v_param.datatype_schema) || ' p_' || v_ep.source_relation_parameter_id);

    END LOOP;

    RAISE DEBUG 'v_exp_test_select_list: %',v_exp_test_select_list;
    
    IF cardinality(v_exp_test_select_list) > 0 THEN
        v_exp_test := format('WITH ct AS (SELECT %s FROM datatypes) SELECT %s as col1 FROM ct',array_to_string(v_exp_test_select_list,','), v_ret_expression);
    ELSE -- no parameters
        v_exp_test := format('SELECT %s as col1', v_ret_expression);
    END IF;

    RETURN v_exp_test;

END;

$BODY$;

-- for each key in in_object, replaces if with value form a matching key in in_add_object if it's not null. Runs recursively for jsonb keys
CREATE OR REPLACE FUNCTION meta.u_append_object(in_object jsonb, in_add_object jsonb)
    RETURNS jsonb
    LANGUAGE plpgsql
AS
$function$

DECLARE
    v_obj jsonb := in_object;
    v_key text;
    v_value jsonb;
BEGIN

    FOR v_key,v_value IN SELECT k, v FROM jsonb_each(in_object) o(k,v) WHERE in_add_object ->> o.k IS NOT NULL
        LOOP 
            IF jsonb_typeof( v_value ) = 'object' THEN 
                v_obj := jsonb_set(v_obj, ARRAY[v_key], meta.u_append_object(in_object -> v_key, in_add_object -> v_key));
            ELSE
                v_obj := jsonb_set(v_obj, ARRAY[v_key], in_add_object->v_key);
            END IF;
        END LOOP;

RETURN v_obj;
END;

$function$;CREATE OR REPLACE FUNCTION meta.u_array_starts_with(in_test int[], in_starts_with int[])
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$

DECLARE
    v_length_startswith int = cardinality(in_starts_with);
    v_length_test int = cardinality(in_test);
    v_index int;
BEGIN

IF v_length_startswith = 0 THEN
    RETURN true;
ELSEIF v_length_startswith > v_length_test THEN
    RETURN false;
END IF;

FOR v_index IN 1..v_length_startswith LOOP
    IF in_test[v_index] <> in_starts_with[v_index] THEN 
        RETURN false;
    END IF;
END LOOP;

RETURN true;



END;
$function$;CREATE OR REPLACE FUNCTION meta.u_assert(in_assert_boolean boolean, in_message text)
 RETURNS void
 LANGUAGE plpgsql
 COST 100
AS $function$
BEGIN
    IF NOT COALESCE(in_assert_boolean,false) THEN
        RAISE EXCEPTION '%', in_message;
    END IF;
END;
$function$;
CREATE OR REPLACE FUNCTION meta.u_build_datatype_test_expr(in_expression text)
    RETURNS text
    LANGUAGE 'plpgsql'

AS
$BODY$

DECLARE
    v_end                       int;
    v_last_end                  int := 1;
    v_datatype                  text;
    v_add                       text;
    v_datatype_schema           jsonb;
    v_aggregate_id               int;
    v_aggregates_exist_flag     boolean;
    v_parameter_id               int;
    v_start                     int;
    v_exp_test_select_list      text[] := '{}';
    v_exp_test                  text;
    v_ret_expression            text    := ''; -- test expression 

BEGIN

    v_aggregates_exist_flag := EXISTS(SELECT 1 FROM _aggs_parsed);

    -- build test expression in this format:
    -- WITH ct AS (SELECT <exp1> p_1, <exp2> p_2 FROM datatypes)
    -- SELECT <expr with P<1> replaced with p_1> as col1 FROM ct

    FOR v_parameter_id, v_start, v_end, v_datatype, v_aggregate_id, v_datatype_schema
        IN SELECT id, p_start, p_end, datatype, aggregation_id, datatype_schema
           FROM _params ORDER BY id LOOP
        IF v_datatype IS NULL THEN
            RETURN NULL;
        END IF;

        -- add preceding characters
        RAISE DEBUG 'v_parameter_id %  start % end % last end %', v_parameter_id,  v_start, v_end, v_last_end;
        v_add := substr(in_expression,v_last_end, v_start - v_last_end);
        v_ret_expression := v_ret_expression || v_add;

        -- add parameter with datatype
        v_exp_test_select_list := v_exp_test_select_list ||
         (CASE WHEN v_aggregates_exist_flag AND v_aggregate_id IS NULL 
            THEN  'first_value(' || meta.u_datatype_test_expression(v_datatype,v_datatype_schema) || ')' -- wrap non-aggregated parameter into aggregate for data type testing purposes only
            ELSE  meta.u_datatype_test_expression(v_datatype,v_datatype_schema) END  || ' p_' || v_parameter_id);

        v_ret_expression := v_ret_expression || 'p_' || v_parameter_id;

        v_last_end = v_end + 1;
    END LOOP;
    -- add remaining trailing charaters
    v_ret_expression := v_ret_expression || substr(in_expression,v_last_end);

    RAISE DEBUG 'v_exp_test_select_list: %',v_exp_test_select_list;
    
    IF cardinality(v_exp_test_select_list) > 0 THEN
        v_exp_test := format('WITH ct AS (SELECT %s FROM datatypes) SELECT %s as col1 FROM ct',array_to_string(v_exp_test_select_list,','), v_ret_expression);
    ELSE -- no parameters
        v_exp_test := format('SELECT %s as col1', v_ret_expression);
    END IF;


    RETURN v_exp_test;

END;

$BODY$;

CREATE OR REPLACE FUNCTION meta.u_compare_nulls(in_arg1 anyelement, in_arg2 anyelement)
    RETURNS boolean
    IMMUTABLE
    LANGUAGE plpgsql
AS
$function$

BEGIN

RETURN (in_arg1 IS NULL AND in_arg2 IS NULL) OR (in_arg1 IS NOT NULL AND in_arg2 IS NOT NULL);
    
END;
$function$;

-- build constant expression for testing parameter data type 
CREATE OR REPLACE FUNCTION meta.u_datatype_test_expression(in_datatype text, in_datatype_schema jsonb)
    RETURNS text
    LANGUAGE 'plpgsql'

AS
$BODY$

DECLARE
    v_fields jsonb;
    v_array_type jsonb;
    v_array_sub_exp text;
    v_exp text;
BEGIN

IF in_datatype = 'struct' THEN 
    PERFORM meta.u_assert(in_datatype_schema->>'type' = 'struct',format('datatype_schema %s does not match datatype %s',in_datatype_schema,in_datatype));
    v_fields := in_datatype_schema->'fields';
    PERFORM meta.u_assert(jsonb_typeof(v_fields) = 'array' ,format('Invalid fields value in datatype_schema %s for datatype %s',in_datatype_schema,in_datatype));

    -- Create expression as struct( field1.typeExp AS field1.name, field2.typeExp AS field2.name, ... )

    SELECT 'struct(' || string_agg(CASE WHEN jsonb_typeof(field->'type') = 'string' THEN format('%s AS %s',meta.u_datatype_test_expression(field->>'type', null), field->>'name') 
        WHEN jsonb_typeof(field->'type') = 'object' THEN format('%s AS %s',meta.u_datatype_test_expression(field->'type'->>'type', field->'type'), field->>'name') 
        ELSE format('ERROR: Invalid type %s of field %s',field->>'type', field->>'name') END,', ') || ')'
    INTO v_exp
    FROM jsonb_array_elements(v_fields) field;

    PERFORM meta.u_assert(v_exp IS NOT NULL ,format('Null expression in datatype_schema %s for datatype %s',in_datatype_schema,in_datatype));
    PERFORM meta.u_assert(v_exp NOT LIKE 'ERROR: Invalid type %' ,format('Error in expression %s in datatype_schema %s for datatype %s',v_exp ,in_datatype_schema,in_datatype));

ELSEIF in_datatype = 'array' THEN 
    PERFORM meta.u_assert(in_datatype_schema->>'type' = 'array',format('datatype_schema %s does not match datatype %s',in_datatype_schema,in_datatype));
    v_array_type := in_datatype_schema->'elementType';

    -- Create expression as array( element1, element2 )
    IF jsonb_typeof(in_datatype_schema->'elementType') = 'string' THEN -- array of simple type
        v_array_sub_exp := meta.u_datatype_test_expression(in_datatype_schema->>'elementType', null); 
    ELSEIF jsonb_typeof(in_datatype_schema->'elementType') = 'object' THEN -- nested array
        v_array_sub_exp := meta.u_datatype_test_expression(in_datatype_schema->'elementType'->>'type', in_datatype_schema->'elementType');
    ELSE
        PERFORM meta.u_assert(false, format('Invalid array elementType: %s'),in_datatype_schema->>'elementType');
    END IF;

    v_exp := format('array(%s,%s)',v_array_sub_exp,v_array_sub_exp);

ELSEIF in_datatype like 'decimal(%' THEN 
    v_exp :=  format('CAST(`decimal` AS %s)',in_datatype);
ELSE
   v_exp :=  '`' || in_datatype || '`';
END IF;   

RETURN v_exp;

END;

$BODY$;CREATE OR REPLACE FUNCTION meta.u_enr_query_get_enrichment_parameter_name(in_parameter meta.enrichment_parameter)
 RETURNS text -- returns attribute name
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
v_attribute_name text;
BEGIN
    IF in_parameter.type = 'enrichment' THEN
        PERFORM meta.u_assert( in_parameter.enrichment_id IS NOT NULL, 'enrichment_id is NULL for enrichment_parameter_id=' || in_parameter.enrichment_parameter_id);
        SELECT e.attribute_name INTO v_attribute_name
        FROM meta.enrichment e WHERE e.enrichment_id = in_parameter.enrichment_id;
        PERFORM meta.u_assert( v_attribute_name IS NOT NULL, 'enrichment_id=' || in_parameter.enrichment_id || 'referenced by enrichment_parameter_id' || in_parameter.enrichment_parameter_id || ' does not exist');
    ELSEIF in_parameter.type = 'raw' THEN
        PERFORM meta.u_assert( in_parameter.raw_attribute_id IS NOT NULL, 'raw_attribute_id is NULL for enrichment_parameter_id=' || in_parameter.enrichment_parameter_id);
        SELECT r.column_alias INTO v_attribute_name
        FROM meta.raw_attribute r WHERE r.raw_attribute_id = in_parameter.raw_attribute_id;
        PERFORM meta.u_assert( v_attribute_name IS NOT NULL, 'raw_attribute_id=' || in_parameter.raw_attribute_id || 'referenced by enrichment_parameter_id' || in_parameter.enrichment_parameter_id || ' does not exist');
    ELSEIF in_parameter.type = 'system' THEN
        PERFORM meta.u_assert( in_parameter.system_attribute_id IS NOT NULL, 'system_attribute_id is NULL for enrichment_parameter_id=' || in_parameter.enrichment_parameter_id);
        SELECT s.name INTO v_attribute_name
        FROM meta.system_attribute s WHERE s.system_attribute_id = in_parameter.system_attribute_id;
        PERFORM meta.u_assert( v_attribute_name IS NOT NULL, 'system_attribute_id=' || in_parameter.system_attribute_id || 'referenced by enrichment_parameter_id' || in_parameter.enrichment_parameter_id || ' does not exist');
    END IF;

RETURN v_attribute_name;
END;

$function$;

CREATE OR REPLACE FUNCTION meta.u_enr_query_get_enrichment_parameter_name(in_parameter meta.source_relation_parameter)
 RETURNS text -- returns attribute name
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
v_attribute_name text;
BEGIN
    IF in_parameter.type = 'enrichment' THEN
        PERFORM meta.u_assert( in_parameter.enrichment_id IS NOT NULL, 'enrichment_id is NULL for source_relation_parameter_id=' || in_parameter.source_relation_parameter_id);
        SELECT e.attribute_name INTO v_attribute_name
        FROM meta.enrichment e WHERE e.enrichment_id = in_parameter.enrichment_id;
        PERFORM meta.u_assert( v_attribute_name IS NOT NULL, 'enrichment_id=' || in_parameter.enrichment_id || 'referenced by source_relation_parameter_id' || in_parameter.source_relation_parameter_id || ' does not exist');
    ELSEIF in_parameter.type = 'raw' THEN
        PERFORM meta.u_assert( in_parameter.raw_attribute_id IS NOT NULL, 'raw_attribute_id is NULL for source_relation_parameter_id=' || in_parameter.source_relation_parameter_id);
        SELECT r.column_alias INTO v_attribute_name
        FROM meta.raw_attribute r WHERE r.raw_attribute_id = in_parameter.raw_attribute_id;
        PERFORM meta.u_assert( v_attribute_name IS NOT NULL, 'raw_attribute_id=' || in_parameter.raw_attribute_id || 'referenced by source_relation_parameter_id' || in_parameter.source_relation_parameter_id || ' does not exist');
    ELSEIF in_parameter.type = 'system' THEN
        PERFORM meta.u_assert( in_parameter.system_attribute_id IS NOT NULL, 'system_attribute_id is NULL for source_relation_parameter_id=' || in_parameter.source_relation_parameter_id);
        SELECT s.name INTO v_attribute_name
        FROM meta.system_attribute s WHERE s.system_attribute_id = in_parameter.system_attribute_id;
        PERFORM meta.u_assert( v_attribute_name IS NOT NULL, 'system_attribute_id=' || in_parameter.system_attribute_id || 'referenced by source_relation_parameter_id' || in_parameter.source_relation_parameter_id || ' does not exist');
    END IF;

RETURN v_attribute_name;
END;

$function$;
CREATE OR REPLACE FUNCTION meta.u_get_next_hop(in_relation_ids int[], in_level int, in_start_path int[])
 RETURNS TABLE (next_relation_id int, next_complete_flag boolean, next_hop_exists_flag boolean, reverse_flag boolean)
 LANGUAGE plpgsql
AS $function$

BEGIN

	DROP TABLE IF EXISTS _next_hop;
	CREATE TEMP TABLE _next_hop ON COMMIT DROP AS
		SELECT relation_ids[in_level] relation_id, -- next relation in path
		bool_or(cardinality(relation_ids) = in_level) as complete_flag, -- TRUE when this relation forms complate path which ends at the destination source
		bool_or(sr.primary_flag) primary_flag, 
		count(1) > 1 as next_hop_exists_flag, -- TRUE when more relations can be added to existing path
		min(path_level) min_length,
		reverse_flags[in_level] reverse_flag
		FROM _paths JOIN meta.source_relation sr ON sr.source_relation_id = relation_ids[in_level]
		WHERE meta.u_array_starts_with(relation_ids,in_relation_ids) -- keep only paths we found on prior iterations
		GROUP BY relation_ids[in_level],reverse_flags[in_level];

	RETURN QUERY (
		SELECT n.relation_id, n.complete_flag, n.next_hop_exists_flag, n.reverse_flag
		FROM _next_hop n
		WHERE (cardinality(in_start_path) < in_level OR in_start_path[in_level] = relation_id ) 
			--AND (v_level = 1 OR v_next_relation_id IS DISTINCT FROM relation_id)
		ORDER BY primary_flag DESC, complete_flag DESC, min_length 
		LIMIT 1		
	);
	
END;

$function$;


DROP FUNCTION IF EXISTS meta.u_get_next_relation_path;
CREATE OR REPLACE FUNCTION meta.u_get_next_relation_path(in_from_source_id int, in_to_source_id int, 
in_cardinality text = '1', in_start_path int[] = '{}', in_max_length int = 10)
 RETURNS json
 LANGUAGE plpgsql
AS $function$

DECLARE
	v_in_path_length int;
	v_in_path_complete boolean = false;
	v_level int;
	v_max_level int;
	v_next_relation_id int;
	v_next_complete_flag boolean;
	v_ret_selections json;
	v_ret_path jsonb := '[]';
	v_multi_path_flag boolean;
	v_next_hop_exists_flag boolean;
	v_missing_relation_ids int[];
	v_ret_relation_ids int[] := '{}';
	v_reverse_flag boolean;
BEGIN

in_start_path := COALESCE(in_start_path,'{}'::int[]);
v_in_path_length := cardinality(in_start_path);
-- check if relations in in_start_path exist and are active

in_max_length := greatest(in_max_length, v_in_path_length + 2);

SELECT array_agg(r.id) INTO v_missing_relation_ids
FROM unnest(in_start_path) r(id)
LEFT JOIN meta.source_relation sr ON sr.source_relation_id = r.id AND sr.active_flag
WHERE sr.source_relation_id IS NULL;

IF v_missing_relation_ids IS NOT NULL THEN
	RETURN json_build_object('error','Relation id(s) do not exist or are not active: ' || v_missing_relation_ids::text);
END IF;

DROP TABLE IF EXISTS _paths;
CREATE TEMP TABLE _paths ON COMMIT DROP 
AS
WITH recursive ct AS (
	SELECT ca.next_source_id, ca.cardinality,
	ARRAY[ca.source_relation_id] relation_ids, 1 path_level, ARRAY[ca.reverse_flag] reverse_flags,
	ca.one_to_one_flag, ca.source_relation_id
	FROM meta.u_relation_with_cardinality(in_from_source_id) ca
	UNION

	SELECT ca.next_source_id, ca.cardinality,
	ct.relation_ids || ca.source_relation_id relation_ids, ct.path_level + 1, ct.reverse_flags ||  ca.reverse_flag,
	ca.one_to_one_flag, ca.source_relation_id
		FROM ct CROSS JOIN meta.u_relation_with_cardinality(ct.next_source_id) ca
		WHERE ct.cardinality = '1' -- all relations prior to last one in the chain must be 1
		AND ct.path_level <= in_max_length -- + v_in_path_length
		-- prevent multiple traverse of same relation
		AND (NOT ca.source_relation_id = ANY(ct.relation_ids) -- different relation
			OR in_start_path[ct.path_level + 1] = ca.source_relation_id -- allow relation that came in via source path
			OR in_start_path[ct.path_level] = ca.source_relation_id -- allow manual chaining of repeated relations
			)
)
SELECT relation_ids,path_level, cardinality, reverse_flags
FROM ct
WHERE next_source_id = in_to_source_id; 

RAISE DEBUG 'from source_id % to source_id % _paths COUNT %', in_from_source_id, in_to_source_id, (select count(1) from _paths);

SELECT max(path_level) INTO v_max_level FROM _paths WHERE cardinality = in_cardinality;

IF v_max_level IS NULL THEN
	-- check if paths with diff catdinality exist
	IF NOT EXISTS(SELECT 1 FROM _paths) THEN
		RETURN json_build_object('error',format('No active relation paths exist from source `%s` to source `%s` with cardinality %s using start path %s',
		meta.u_get_source_name(in_from_source_id) , meta.u_get_source_name(in_to_source_id) , in_cardinality , in_start_path));
	ELSEIF in_cardinality = '1' THEN
		RETURN json_build_object('error',format('You must use aggregation for this parameter. Target source `%s`', meta.u_get_source_name(in_to_source_id) ));
	ELSEIF in_cardinality = 'M' THEN
		RETURN json_build_object('error',format('Remove aggregation from this parameter. Target source `%s`', meta.u_get_source_name(in_to_source_id) ));
	END IF;
ELSE
	DELETE FROM _paths WHERE cardinality != in_cardinality;
END IF;


FOR v_level IN 1 .. v_max_level LOOP
	-- get next hop relations
	-- auto-pick next hop: use passed selected path, don't traverse self-relation more than once
	-- prioritize primary hops, then shortest paths
		SELECT (meta.u_get_next_hop(v_ret_relation_ids, v_level, in_start_path)).*
		INTO v_next_relation_id, v_next_complete_flag, v_next_hop_exists_flag, v_reverse_flag;

		IF v_next_relation_id IS NULL THEN
			RETURN json_build_object('error', format('No relations exist for the next relation level %s. Starting path %s Current path %s',v_level,in_start_path,v_ret_relation_ids));
		END IF;

		-- get all available selections
		SELECT json_agg(sel)
		INTO v_ret_selections
		FROM (
			SELECT jsonb_build_object('relation_id', n.relation_id) || meta.u_get_relation_label(n.relation_id, n.reverse_flag) sel
			FROM _next_hop n
		) r;


	v_ret_path := v_ret_path || (jsonb_build_object('relation_id', v_next_relation_id, 'selections', v_ret_selections,'complete',v_next_complete_flag) ||  meta.u_get_relation_label(v_next_relation_id, v_reverse_flag));
	v_ret_relation_ids := v_ret_relation_ids || v_next_relation_id;

	IF v_next_complete_flag AND v_level >= v_in_path_length THEN
		IF v_next_hop_exists_flag AND v_level < v_max_level THEN
			-- pick single best reltion_id for the next selection
			SELECT (meta.u_get_next_hop(v_ret_relation_ids, v_level + 1, in_start_path)).*
			INTO v_next_relation_id, v_next_complete_flag, v_next_hop_exists_flag, v_reverse_flag;

		ELSE	
			v_next_relation_id := null;
		END IF;
		RETURN json_build_object('path',v_ret_path,'complete', true, 'relation_ids',v_ret_relation_ids, 'next', v_next_relation_id);
	END IF;
END LOOP;

RETURN json_build_object('error','Reached end of loop. Level=' || coalesce(v_level::text,'null') || '. Current relation path ' || COALESCE(v_ret_path::text,'[]'));

END;

$function$;

CREATE OR REPLACE FUNCTION meta.u_get_output_parameter_name_id(in_parameter meta.output_source_column)
 RETURNS TABLE(attribute_name text, id int, source_id int) -- returns attribute name, id and source_id
 LANGUAGE plpgsql
 COST 10
AS $function$

BEGIN
    IF in_parameter.type = 'enrichment' THEN
        PERFORM meta.u_assert( in_parameter.enrichment_id IS NOT NULL, 'enrichment_id is NULL for output_source_column_id=' || in_parameter.output_source_column_id);
        RETURN QUERY
        SELECT e.attribute_name, e.enrichment_id, e.source_id 
        FROM meta.enrichment e WHERE e.enrichment_id = in_parameter.enrichment_id;
    ELSEIF in_parameter.type = 'raw' THEN
        PERFORM meta.u_assert( in_parameter.raw_attribute_id IS NOT NULL, 'raw_attribute_id is NULL for output_source_column_id=' || in_parameter.output_source_column_id);
        RETURN QUERY
        SELECT r.column_alias, r.raw_attribute_id, r.source_id
        FROM meta.raw_attribute r WHERE r.raw_attribute_id = in_parameter.raw_attribute_id;
    ELSEIF in_parameter.type = 'system' THEN
        PERFORM meta.u_assert( in_parameter.system_attribute_id IS NOT NULL, 'system_attribute_id is NULL for output_source_column_id=' || in_parameter.output_source_column_id);
        RETURN QUERY
        SELECT sy.name, sy.system_attribute_id, os.source_id 
        FROM meta.system_attribute sy JOIN meta.output_source os ON os.output_source_id = in_parameter.output_source_id
        WHERE sy.system_attribute_id = in_parameter.system_attribute_id;
    END IF;

RETURN;
END;

$function$;
CREATE OR REPLACE FUNCTION meta.u_get_parameter(in_p meta.enrichment_parameter)
 RETURNS parameter_map
 LANGUAGE plpgsql
AS $function$

DECLARE
       v_ret parameter_map = ROW(in_p.type, in_p.raw_attribute_id, in_p.enrichment_id, in_p.system_attribute_id, in_p.source_id, null::text, null::text, null::jsonb)::parameter_map;
BEGIN
    
    IF in_p.type = 'raw' THEN
            SELECT r.data_type, r.datatype_schema 
            INTO v_ret.datatype, v_ret.datatype_schema
            FROM meta.raw_attribute r 
            WHERE r.raw_attribute_id = in_p.raw_attribute_id;
    ELSEIF in_p.type = 'enrichment' THEN
            SELECT e.datatype, e.datatype_schema  
            INTO v_ret.datatype, v_ret.datatype_schema
            FROM meta.enrichment e 
            WHERE e.enrichment_id = in_p.enrichment_id;
    ELSEIF in_p.type = 'system' THEN
            SELECT data_type, meta.u_get_schema_from_type(null, data_type)
            INTO v_ret.datatype, v_ret.datatype_schema
            FROM meta.system_attribute s
            WHERE s.system_attribute_id = in_p.system_attribute_id;
    ELSE    
        v_ret.error := 'Invalid parameter ' || in_p::text;
    END IF;

    RETURN v_ret;

END;

$function$;

CREATE OR REPLACE FUNCTION meta.u_get_parameter(in_p meta.source_relation_parameter)
 RETURNS parameter_map
 LANGUAGE plpgsql
AS $function$

DECLARE
       v_ret parameter_map = ROW(in_p.type, in_p.raw_attribute_id, in_p.enrichment_id, in_p.system_attribute_id, in_p.source_id, null::text, null::text, null::jsonb)::parameter_map;
BEGIN
    
    IF in_p.type = 'raw' THEN
            SELECT r.data_type, r.datatype_schema 
            INTO v_ret.datatype, v_ret.datatype_schema
            FROM meta.raw_attribute r 
            WHERE r.raw_attribute_id = in_p.raw_attribute_id;
    ELSEIF in_p.type = 'enrichment' THEN
            SELECT e.datatype, e.datatype_schema  
            INTO v_ret.datatype, v_ret.datatype_schema
            FROM meta.enrichment e 
            WHERE e.enrichment_id = in_p.enrichment_id;
    ELSEIF in_p.type = 'system' THEN
            SELECT data_type, meta.u_get_schema_from_type(null, data_type)
            INTO v_ret.datatype, v_ret.datatype_schema
            FROM meta.system_attribute s
            WHERE s.system_attribute_id = in_p.system_attribute_id;
    ELSE    
        v_ret.error := 'Invalid parameter ' || in_p::text;
    END IF;

    RETURN v_ret;

END;

$function$;CREATE OR REPLACE  FUNCTION meta.u_get_relation_label(in_source_relation_id int, in_reverse_flag boolean)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$

BEGIN

RETURN (
	SELECT jsonb_build_object('label',  '[' || CASE WHEN in_reverse_flag THEN rs.source_name ELSE s.source_name END || E'] \u2192 ' || sr.relation_name || E' \u2192 [' || CASE WHEN in_reverse_flag THEN s.source_name ELSE rs.source_name END || ']',
	'expression',
	CASE WHEN sr.source_id <> sr.related_source_id THEN replace(replace(sr.expression, '[This]', '[' || s.source_name || ']'), '[Related]', '[' || rs.source_name || ']')
	ELSE replace(sr.expression, '[This]', '[' || s.source_name  || ']') 
	END )
	FROM meta.source_relation sr
	JOIN meta.source s ON s.source_id = sr.source_id
	JOIN meta.source rs ON rs.source_id = sr.related_source_id
	WHERE sr.source_relation_id = in_source_relation_id
);
	
	
END;

$function$;

-- return relation path from _impc_enrichment_parameter table
CREATE OR REPLACE FUNCTION meta.u_get_relation_path_core(in_enrichment_id int, in_source_name text)
 RETURNS int[]
 LANGUAGE plpgsql
AS $function$

BEGIN

IF NOT EXISTS(SELECT 1 FROM pg_tables where tablename = '_impc_enrichment_parameter') THEN
	RETURN null;
END IF;

RETURN (
	SELECT source_relation_ids
	FROM _impc_enrichment_parameter p
	WHERE p.enrichment_id = in_enrichment_id AND
	COALESCE(p.source_name,in_source_name) = in_source_name
);
	
END;

$function$;

-- build and return datatype_schema if it's null (legacy pre-8.0 attribute)
CREATE OR REPLACE FUNCTION meta.u_get_schema_from_type(in_schema jsonb, in_datatype text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    
AS $BODY$

DECLARE

     v_type text;

BEGIN

IF in_schema IS NOT NULL THEN
    RETURN in_schema;
END IF;


IF in_datatype = 'int' THEN
    RETURN to_jsonb('integer'::text);
ELSEIF in_datatype LIKE 'decimal%' THEN 
    RETURN to_jsonb('decimal(38,12)'::text);
END IF;

RETURN to_jsonb(in_datatype);

END;

$BODY$;
CREATE OR REPLACE FUNCTION meta.u_get_source_name(in_source_id int)
 RETURNS text
 LANGUAGE plpgsql
AS $function$

BEGIN
	
RETURN COALESCE(
	(SELECT s.source_name FROM meta.source s WHERE s.source_id = in_source_id),
	format('Unknown source_id=%s', in_source_id));

END;

$function$;
CREATE OR REPLACE FUNCTION meta.u_get_source_project(in_source_id int)
 RETURNS int
 LANGUAGE plpgsql
 COST 10
AS $function$

BEGIN

RETURN (SELECT project_id FROM meta.source WHERE source_id = in_source_id);
END;

$function$;
-- lookup complex attribute in source, then lookup key and return it's datatype or error
CREATE OR REPLACE FUNCTION meta.u_get_struct_key_datatype(in_datatype_schema jsonb, in_keys text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
AS $BODY$

DECLARE
    v_key text;
    v_struct_keys_array text[];
    v_parameter parameter_map;
    v_schema jsonb := in_datatype_schema;
BEGIN

IF in_datatype_schema IS NULL THEN
    RETURN jsonb_build_object('error', 'Datatype schema is NULL');
END IF;

    IF COALESCE(in_keys,'') != '' THEN
    
        v_struct_keys_array := regexp_split_to_array(in_keys,'\.');

        FOREACH v_key IN ARRAY v_struct_keys_array LOOP
            RAISE DEBUG 'Key % schema %', v_key, v_schema;
            IF v_schema->>'type' IS DISTINCT FROM 'struct' THEN
                RETURN jsonb_build_object('error', format('Non-struct attribute cannot be accessed by .%s key',v_key));
            END IF;

            SELECT f->'type'
            INTO v_schema
            FROM jsonb_array_elements(v_schema->'fields') f
            WHERE f->>'name' = v_key;

            IF v_schema IS NULL THEN
                RETURN jsonb_build_object('error', format('Unable to lookup struct key `%s`. Check your expression',v_key));
            END IF;

        END LOOP;

    END IF;


    RETURN jsonb_build_object('datatype', COALESCE(v_schema->>'type',REPLACE(v_schema->>0,'integer','int')),
                              'datatype_schema',v_schema);

END;

$BODY$;
-- get datatype name from datatype_schema
CREATE OR REPLACE FUNCTION meta.u_get_typename_from_schema(in_schema jsonb)
    RETURNS text
    LANGUAGE 'plpgsql'
    
AS $BODY$

DECLARE

     v_type text;

BEGIN

v_type := CASE WHEN jsonb_typeof(in_schema) = 'string' THEN in_schema->>0
        WHEN jsonb_typeof(in_schema) = 'object' THEN meta.u_get_typename_from_schema(in_schema->'type') 
        END;


IF v_type IN ('integer', 'byte','short') THEN
    v_type := 'int';
ELSEIF v_type LIKE 'decimal%' THEN 
    v_type := 'decimal';
END IF;

RETURN v_type;

END;

$BODY$;CREATE OR REPLACE FUNCTION meta.u_insert_source_relation_parameters(in_field_name text, in_source_relation_id INT, in_attribute_type text,
                                                               in_id INT, in_source_id INT, in_base_source_flag boolean)
    RETURNS JSONB
    LANGUAGE plpgsql
    COST 10
AS
$function$

DECLARE
    v_source_relation_parameter_id int;
    v_self_relation_flag boolean;
    v_existing_parameter_id int;

BEGIN

    IF in_source_relation_id IS NULL THEN
        -- don't insert parameters and return dummy expression - this is check only
        RETURN jsonb_build_object('expression', CASE WHEN in_base_source_flag THEN '[This].' ELSE '[Related].' END || in_field_name, 'id', 0);
    END IF;

    SELECT sr.source_id = sr.related_source_id
    INTO v_self_relation_flag
        FROM meta.source_relation sr
    WHERE source_relation_id = in_source_relation_id;

    SELECT source_relation_parameter_id
        INTO v_existing_parameter_id
    FROM meta.source_relation_parameter
        WHERE source_relation_id = in_source_relation_id
        AND CASE
            WHEN in_attribute_type = 'raw' THEN type = 'raw' AND raw_attribute_id = in_id
            WHEN in_attribute_type = 'enrichment' THEN type = 'enrichment' AND enrichment_id = in_id
            WHEN in_attribute_type = 'system' THEN type = 'system' AND system_attribute_id = in_id
            END
        AND source_id = in_source_id
        AND self_relation_container IS NOT DISTINCT FROM CASE WHEN v_self_relation_flag THEN CASE WHEN in_base_source_flag THEN 'This' ELSE 'Related' END END;

    IF v_existing_parameter_id IS NOT NULL THEN
            RETURN jsonb_build_object('expression', CASE WHEN in_base_source_flag THEN '[This].' ELSE '[Related].' END || in_field_name, 'id', v_existing_parameter_id);

    ELSE
        -- get max Id
        SELECT COALESCE(MAX(source_relation_parameter_id),0)
        INTO v_existing_parameter_id
        FROM meta.source_relation_parameter
        WHERE source_relation_id = in_source_relation_id;

        INSERT INTO meta.source_relation_parameter (source_relation_parameter_id, source_relation_id, type, enrichment_id, raw_attribute_id, system_attribute_id, source_id, self_relation_container) VALUES
        (v_existing_parameter_id + 1, in_source_relation_id, in_attribute_type, CASE WHEN in_attribute_type = 'enrichment' THEN in_id END, CASE WHEN in_attribute_type = 'raw' THEN in_id END,CASE WHEN in_attribute_type = 'system' THEN in_id END, in_source_id ,CASE WHEN v_self_relation_flag THEN CASE WHEN in_base_source_flag THEN 'This' ELSE 'Related' END END)
            RETURNING source_relation_parameter_id INTO v_source_relation_parameter_id;

        RETURN jsonb_build_object('expression', CASE WHEN in_base_source_flag THEN '[This].' ELSE '[Related].' END || in_field_name, 'id', v_source_relation_parameter_id);

    END IF;
END;

$function$;

CREATE OR REPLACE FUNCTION meta.u_insert_source_relation_parameters(in_parameter parameter_map, in_source_relation_id INT, in_base_source_flag boolean)
    RETURNS int
    LANGUAGE plpgsql
AS
$function$

DECLARE
    v_source_relation_parameter_id int;
    v_self_relation_flag boolean;
    v_existing_parameter_id int;

BEGIN

    SELECT sr.source_id = sr.related_source_id
    INTO v_self_relation_flag
    FROM meta.source_relation sr
    WHERE source_relation_id = in_source_relation_id;

    SELECT source_relation_parameter_id
    INTO v_existing_parameter_id
    FROM meta.source_relation_parameter
        WHERE source_relation_id = in_source_relation_id
        AND CASE
            WHEN in_parameter.type = 'raw' THEN type = 'raw' AND raw_attribute_id = in_parameter.raw_attribute_id
            WHEN in_parameter.type = 'enrichment' THEN type = 'enrichment' AND enrichment_id = in_parameter.enrichment_id
            WHEN in_parameter.type = 'system' THEN type = 'system' AND system_attribute_id = in_parameter.system_attribute_id
            END
        AND source_id = in_parameter.source_id
        AND self_relation_container IS NOT DISTINCT FROM CASE WHEN v_self_relation_flag THEN CASE WHEN in_base_source_flag THEN 'This' ELSE 'Related' END END;

    IF v_existing_parameter_id IS NOT NULL THEN
        RETURN v_existing_parameter_id;
    ELSE
        -- get max Id
        SELECT COALESCE(MAX(source_relation_parameter_id),0)
        INTO v_existing_parameter_id
        FROM meta.source_relation_parameter
        WHERE source_relation_id = in_source_relation_id;

        INSERT INTO meta.source_relation_parameter (source_relation_parameter_id, source_relation_id, type, enrichment_id, raw_attribute_id, system_attribute_id, source_id, self_relation_container) VALUES
        (v_existing_parameter_id + 1, in_source_relation_id, in_parameter.type,  in_parameter.enrichment_id, in_parameter.raw_attribute_id, in_parameter.system_attribute_id, in_parameter.source_id ,CASE WHEN v_self_relation_flag THEN CASE WHEN in_base_source_flag THEN 'This' ELSE 'Related' END END)
            RETURNING source_relation_parameter_id INTO v_source_relation_parameter_id;

        RETURN  v_source_relation_parameter_id;

    END IF;
END;

$function$;
CREATE OR REPLACE FUNCTION meta.u_json_array_to_int_array(in_parameters json)
 RETURNS int[]
 LANGUAGE plpgsql
AS $function$

DECLARE
v_ret int[];
BEGIN
    IF in_parameters::jsonb IS DISTINCT FROM 'null'::jsonb THEN
        SELECT array_agg(t::int) INTO v_ret FROM json_array_elements_text(in_parameters) t;
        RETURN v_ret;
    END IF;

    RETURN null;

END;
$function$;

CREATE OR REPLACE FUNCTION meta.u_lookup_source_attribute(in_source_id int, in_attribute_name text, in_test_source_id int = null)
 RETURNS parameter_map
 LANGUAGE plpgsql
AS $function$

DECLARE
    v_ret parameter_map = ROW(null::text, null::int, null::int, null::int, in_source_id, null::text, null::text, null::jsonb)::parameter_map;
    v_attribute_name_substituted text;
    v_attribute_name_substituted_json json;
BEGIN

    v_attribute_name_substituted := in_attribute_name;
    
    SELECT r.raw_attribute_id, r.data_type, r.datatype_schema 
    INTO v_ret.raw_attribute_id, v_ret.datatype, v_ret.datatype_schema
    FROM meta.raw_attribute r 
    WHERE r.source_id = in_source_id AND r.column_alias = v_attribute_name_substituted;

    IF v_ret.raw_attribute_id IS NOT NULL THEN
        v_ret.type = 'raw';
        RETURN v_ret;
    END IF;

    SELECT enrichment_id, COALESCE(NULLIF(e.cast_datatype,''), e.datatype), datatype_schema 
    INTO v_ret.enrichment_id, v_ret.datatype, v_ret.datatype_schema
    FROM meta.enrichment e 
    WHERE e.source_id = in_source_id AND e.active_flag AND e.attribute_name = v_attribute_name_substituted;

    IF v_ret.enrichment_id IS NOT NULL THEN
        v_ret.type = 'enrichment';
        RETURN v_ret;
    END IF;

    SELECT s.system_attribute_id, s.data_type 
    INTO v_ret.system_attribute_id, v_ret.datatype
    FROM meta.system_attribute s
    JOIN meta.source src  ON src.source_id = in_source_id AND 
    s.refresh_type @> ARRAY[src.refresh_type] AND s.table_type @> ARRAY['hub']
    WHERE s.name = v_attribute_name_substituted;

    IF v_ret.system_attribute_id IS NOT NULL THEN
        v_ret.type = 'system';
        RETURN v_ret;
    END IF;

    v_ret.error := format('Attribute `%s` does not exist in source `%s`',v_attribute_name_substituted, meta.u_get_source_name(in_source_id)); 
    RETURN v_ret;

END;

$function$;


-- lookup complex attribute in source, then lookup key and return it's datatype or error
CREATE OR REPLACE FUNCTION meta.u_lookup_source_attribute(in_source_id int, in_attribute_name text, in_keys text)
    RETURNS parameter_map
    LANGUAGE 'plpgsql'
AS $BODY$

DECLARE
    v_key text;
    v_struct_keys_array text[];
    v_parameter parameter_map;
    v_schema jsonb;
BEGIN

    v_parameter := meta.u_lookup_source_attribute(in_source_id, in_attribute_name);

    IF v_parameter.error IS NOT NULL OR COALESCE(in_keys,'') = '' THEN
        RETURN v_parameter;
    END IF;
    
    v_schema := meta.u_get_struct_key_datatype(v_parameter.datatype_schema, in_keys);

    v_parameter.error = v_schema->>'error';
    v_parameter.datatype := v_schema->>'datatype';
    v_parameter.datatype_schema := v_schema->'datatype_schema';

    RETURN v_parameter;

END;

$BODY$;
-- parse all aggregates in the expression into temp table
CREATE OR REPLACE FUNCTION meta.u_parse_enrichment_aggregates(v_expression text)
    RETURNS text
    LANGUAGE 'plpgsql'

AS
$BODY$

DECLARE
    v_expression_position        INT     := 0;
    v_in_quotes_flag             BOOLEAN := FALSE;
    v_parentheses_depth          INT     := 0;
    v_expression_length          INT;
    v_aggregates_exist_flag      BOOLEAN;
    v_aggregate_name             TEXT;
    v_aggregate_start_position   INT;
    v_char                       CHAR;
    v_prev_char                  CHAR;
    v_next_char                  CHAR;
    v_window_function_flag       BOOLEAN;
    v_in_aggregate_flag          BOOLEAN;
    v_follow_text                TEXT;
    v_inner_text                 TEXT;
    v_inner_text_quoted_removed  TEXT;
    v_aggregates                 TEXT;

BEGIN

SELECT string_agg(aggregate_name,'|') INTO v_aggregates
FROM meta.aggregate;

DROP TABLE IF EXISTS _aggs_parsed;
CREATE TEMP TABLE _aggs_parsed 
    (id int,
    function text,
    a_function_start int,
    a_start int,
    a_end int,
    expression text,
    expression_parsed text,
    relation_ids int[]
    ) ON COMMIT DROP;

    v_expression_length = length(v_expression);

    WHILE v_expression_position < v_expression_length LOOP
        v_expression_position := v_expression_position + 1;
        v_prev_char = CASE WHEN v_expression_position > 1 THEN substring(v_expression, v_expression_position - 1, 1) ELSE ' ' END;
        v_char = substring(v_expression, v_expression_position, 1);
        IF v_in_aggregate_flag AND NOT v_in_quotes_flag THEN
            --Update parenthesis
            IF v_char = ')' THEN
                v_parentheses_depth := v_parentheses_depth - 1;
            ELSEIF v_char = '(' THEN
                v_parentheses_depth := v_parentheses_depth + 1;
            END IF;

            IF v_parentheses_depth = 0
            THEN
                -- Check for window function
                v_follow_text := substring(v_expression, v_expression_position + 1, length(v_expression));
                v_window_function_flag := v_follow_text ~* '^\s*over\s*\(';

                RAISE DEBUG 'Out of parenthesis. position=% v_window_function_flag=% v_follow_text=%',v_expression_position,v_window_function_flag, v_follow_text;
                v_inner_text := substring(v_expression, v_aggregate_start_position, v_expression_position - v_aggregate_start_position);
                v_inner_text_quoted_removed := regexp_replace(v_inner_text, '(''[^'']+'')', '','g');

                -- Check for nested aggregates or window functions
                v_aggregate_name := lower((regexp_match(v_inner_text_quoted_removed,format('(?:^|[^\w])(%s)\(', v_aggregates),'i'))[1]);

                IF v_aggregate_name IS NOT NULL THEN
                   RETURN format('Nested aggregate or window function `%s` found inside inner aggregate expression starting at position %s. Please break it up into separate rule.', v_aggregate_name, v_aggregate_start_position);
                END IF; 

                IF v_window_function_flag THEN
                    IF v_aggregates_exist_flag THEN
                        RETURN 'Cannot have a window function and an aggregate in the same rule. Please break them into separate rules and combine later.Position: ' || v_expression_position;
                    END IF;
                    DELETE FROM _aggs_parsed WHERE a_start = v_aggregate_start_position;
                ELSE
                    v_aggregates_exist_flag := true;
                    --We've got the whole aggregate, update end position
                    UPDATE _aggs_parsed SET a_end = v_expression_position, 
                        expression = v_inner_text
                    WHERE a_start = v_aggregate_start_position;
                END IF;
                v_in_aggregate_flag := FALSE;
            ELSE
                --Still in an aggregate. Keep going.
            END IF;
        
        ELSEIF v_char = '(' AND NOT v_in_quotes_flag THEN 
                --check if we're at the start of aggregate

                v_aggregate_name := lower((regexp_match(substring(v_expression,1, v_expression_position),format('(?:^|[^\w])(%s)\($', v_aggregates),'i'))[1]);

                IF v_aggregate_name IS NOT NULL THEN
                    --we are at the start of aggregate function

                    RAISE DEBUG 'Found aggregate % at position=%',v_aggregate_name, v_expression_position;
                    v_aggregate_start_position := v_expression_position + 1;
            
                    -- save it and move cursor to the beginning of (arguments)
                    INSERT INTO _aggs_parsed(a_start, a_function_start, function) VALUES (v_aggregate_start_position, v_expression_position - length(v_aggregate_name), v_aggregate_name);
                    -- v_expression_position := v_aggregate_start_position - 1;
                    v_parentheses_depth = 1;
                    v_in_aggregate_flag := true;
                END IF;
        END IF;

        IF (v_in_quotes_flag)
        THEN
            IF v_char = ''''
            THEN
                v_next_char := substring(v_expression, v_expression_position + 1, 1);
                IF v_next_char = ''''
                THEN
                    --Double quote escape character. Keep going
                    v_expression_position := v_expression_position + 1;
                ELSE
                    v_in_quotes_flag := FALSE;
                END IF;
            ELSE
                --Still in a quoted string, keep going down the string
            END IF;

        ELSEIF v_char = '''' THEN
                v_in_quotes_flag = TRUE;
        END IF;

    END LOOP;

    IF v_in_aggregate_flag THEN
        RETURN 'Unclosed parenthesis for aggregate starting at position ' || v_aggregate_start_position;
    END IF;


WITH pos AS (SELECT a_start, ROW_NUMBER() OVER(ORDER BY a_start) id FROM _aggs_parsed)
UPDATE _aggs_parsed a SET id = pos.id
FROM pos
WHERE a.a_start = pos.a_start;

UPDATE _params SET aggregation_id = null WHERE aggregation_id NOT IN (SELECT id FROM _aggs_parsed);

RETURN '';

END;

$BODY$;CREATE OR REPLACE FUNCTION meta.u_parse_expression(in_expression text, in_start int, in_end int)
  RETURNS text
    LANGUAGE 'plpgsql'

AS $BODY$
DECLARE

    v_id int;
    v_start int;
    v_end int;
--    v_datatype 
    v_aggregate_id int;
    v_last_end int := in_start;
    v_ret_expression text := '';

BEGIN

    FOR v_id, v_start, v_end   
        IN SELECT id, p_start, p_end
           FROM _params WHERE p_start >= in_start AND p_end <= in_end
           ORDER BY id LOOP
        -- add characters preceding parameter
        v_ret_expression := v_ret_expression || substr(in_expression,v_last_end,v_start - v_last_end);
        
        -- replace parameter with P<n> pointer
        v_ret_expression := v_ret_expression || 'P<' || v_id || '>';
        v_last_end := v_end + 1;
    END LOOP;

    IF v_last_end <= in_end THEN
     -- add remaining trailing charaters
        v_ret_expression := v_ret_expression || substr(in_expression,v_last_end, in_end - v_last_end  + 1);
    END IF;

    RETURN COALESCE(v_ret_expression,format('Null start=%s end=%s',in_start,in_end));

END;

$BODY$;

CREATE OR REPLACE FUNCTION meta.u_read_enrichment_parameters(in_parameters json)
    RETURNS void
    LANGUAGE plpgsql
AS
$function$

BEGIN

DROP TABLE IF EXISTS _params;
-- load parameters into temp table
CREATE TEMP TABLE _params
    (id int, 
    parent_enrichment_id int, 
    type text, enrichment_id int, raw_attribute_id int, system_attribute_id int, source_id int, 
    source_relation_ids int[], 
    self_relation_container text, 
    create_datetime timestamp,
    aggregation_id int,
    source_name text,
    attribute_name text,
    paths json,
    p_start int,
    p_end int,
    datatype text,
    datatype_schema jsonb
    )  ON COMMIT DROP;

IF in_parameters IS NOT NULL THEN
    INSERT INTO _params
    SELECT * FROM json_populate_recordset(null::_params,in_parameters);
END IF;


END;
$function$;

-- returns next relations chainable to in_source_id
CREATE OR REPLACE FUNCTION meta.u_relation_with_cardinality(in_source_id int)
 RETURNS TABLE (source_relation_id int, relation_template_id int, reverse_flag boolean, cardinality text, one_to_one_flag boolean, next_source_id int)
 LANGUAGE plpgsql
AS $function$

BEGIN

RETURN QUERY
SELECT sr.source_relation_id, sr.relation_template_id, 
CASE WHEN sr.source_id = sr.related_source_id OR sr.source_id = in_source_id THEN false ELSE true END reverse_flag, 
CASE WHEN sr.source_id = sr.related_source_id OR sr.source_id = in_source_id THEN sr.related_source_cardinality 
	ELSE sr.source_cardinality END cardinality, 
sr.source_cardinality = '1' AND sr.related_source_cardinality = '1' one_to_one_flag, 
CASE WHEN sr.source_id = in_source_id THEN sr.related_source_id ELSE sr.source_id END next_source_id
FROM meta.source_relation sr 
WHERE in_source_id IN (sr.source_id, sr.related_source_id) AND sr.active_flag;
	
END;

$function$;

CREATE OR REPLACE FUNCTION meta.u_validate_expression_parameters(in_enr meta.enrichment)
    RETURNS text
    LANGUAGE 'plpgsql'
AS $BODY$

DECLARE 
  v_aggregate_ids_check  int[];
  v_param_ids_check  int[];
  v_error text;
BEGIN

--Check that all aggregates use relation_ids
SELECT array_agg(a.enrichment_aggregation_id)::text INTO v_error
FROM meta.enrichment_aggregation a 
WHERE a.enrichment_id = in_enr.enrichment_id AND a.relation_ids IS NULL OR a.relation_ids = '{}'::int[];
IF v_error IS NOT NULL THEN
  RETURN format('Aggregations %s have blank relation_ids. enrichment_id=%s',v_error, in_enr.enrichment_id);
END IF;

--Check that all parameters pointing to [This] source have blank relation_ids
SELECT array_agg(p.enrichment_parameter_id)::text INTO v_error
FROM meta.enrichment_parameter p 
WHERE p.parent_enrichment_id = in_enr.enrichment_id AND p.source_id = in_enr.source_id AND p.self_relation_container IS DISTINCT FROM 'Related'
AND cardinality(p.source_relation_ids) > 0;
IF v_error IS NOT NULL THEN
  RETURN format('[This] source parameters %s have non-blank source_relation_ids. enrichment_id=%s',v_error, in_enr.enrichment_id);
END IF;

--Check that all parameters pointing to non-[This] source have non-blank relation_ids
SELECT array_agg(p.enrichment_parameter_id)::text INTO v_error
FROM meta.enrichment_parameter p 
WHERE p.parent_enrichment_id = in_enr.enrichment_id AND p.source_id <> in_enr.source_id 
AND COALESCE(p.source_relation_ids,'{}'::int[]) = '{}'::int[];
IF v_error IS NOT NULL THEN
  RETURN format('Lookup source parameters %s have blank source_relation_ids. enrichment_id=%s',v_error, in_enr.enrichment_id);
END IF;

--Check that all aggregates relation_ids match those of parameters
SELECT string_agg(format('Aggregation %s Parameter %s',a.enrichment_aggregation_id, p.enrichment_parameter_id),',') INTO v_error
FROM meta.enrichment_aggregation a JOIN meta.enrichment_parameter p ON a.enrichment_aggregation_id = p.aggregation_id
AND a.enrichment_id = p.parent_enrichment_id
WHERE a.enrichment_id = in_enr.enrichment_id AND a.relation_ids <> COALESCE(p.source_relation_ids,a.relation_ids);
IF v_error IS NOT NULL THEN
  RETURN format('Paramters and aggregations have mismatched relation chain: %s enrichment_id=%s',v_error, in_enr.enrichment_id);
END IF;

--Check if all aggregates used in expression_parsed match passed aggregates
SELECT array_agg(m[1]::int) INTO v_aggregate_ids_check FROM regexp_matches(in_enr.expression_parsed, 'A<(\d+)>', 'g') m;

SELECT array_agg(a.enrichment_aggregation_id)::text INTO v_error
FROM meta.enrichment_aggregation a 
WHERE a.enrichment_id = in_enr.enrichment_id AND NOT a.enrichment_aggregation_id = ANY(v_aggregate_ids_check);
IF v_error IS NOT NULL THEN
  RETURN format('Aggregations %s are not referenced in the parsed expression. enrichment_id=%s',v_error, in_enr.enrichment_id);
END IF;

SELECT array_agg(p)::text INTO v_error
FROM unnest(v_aggregate_ids_check) p 
WHERE p NOT IN (SELECT enrichment_aggregation_id FROM meta.enrichment_aggregation a 
WHERE a.enrichment_id = in_enr.enrichment_id);

IF v_error IS NOT NULL THEN
  RETURN format('Aggregations %s referenced in the parsed expression are missing. enrichment_id=%s',v_error, in_enr.enrichment_id);
END IF;

--Check if all parameters used in expression_parsed and aggregate expressions are passed
SELECT array_agg(id) INTO v_param_ids_check 
  FROM (
    SELECT m[1]::int id FROM regexp_matches(in_enr.expression_parsed, 'P<(\d+)>', 'g') m  
    UNION ALL
    SELECT m[1]::int FROM meta.enrichment_aggregation a 
    CROSS JOIN regexp_matches(a.expression, 'P<(\d+)>', 'g') m
    WHERE a.enrichment_id = in_enr.enrichment_id
  ) t;

SELECT array_agg(p.enrichment_parameter_id)::text INTO v_error
FROM meta.enrichment_parameter p WHERE parent_enrichment_id = in_enr.enrichment_id AND NOT p.enrichment_parameter_id = ANY(v_param_ids_check);
IF v_error IS NOT NULL THEN
  RETURN format('Parameters %s are not referenced in the parsed enrichment expression or aggregates. enrichment_id=%s',v_error, in_enr.enrichment_id);
END IF;

SELECT array_agg(p)::text INTO v_error
FROM unnest(v_param_ids_check) p 
WHERE p NOT IN (SELECT enrichment_parameter_id FROM meta.enrichment_parameter p WHERE parent_enrichment_id = in_enr.enrichment_id);

IF v_error IS NOT NULL THEN
  RETURN format('Parameters %s referenced in the parsed expression or aggregate are missing. enrichment_id=%s',v_error, in_enr.enrichment_id);
END IF;

-- Validate that source_id of raw attribute params match 
SELECT array_agg(p.enrichment_parameter_id)::text INTO v_error
FROM meta.enrichment_parameter p 
LEFT JOIN meta.raw_attribute r ON r.raw_attribute_id = p.raw_attribute_id AND r.source_id = p.source_id
WHERE parent_enrichment_id = in_enr.enrichment_id AND p.raw_attribute_id IS NOT NULL AND r.raw_attribute_id IS NULL;
IF v_error IS NOT NULL THEN
  RETURN format('Parameters %s reference raw attribuites that do not match enrichment_parameter.source_id. enrichment_id=%s',v_error, in_enr.enrichment_id);
END IF;

-- Validate that source_id of enrichment params match 
SELECT array_agg(p.enrichment_parameter_id)::text INTO v_error
FROM meta.enrichment_parameter p 
LEFT JOIN meta.enrichment e ON e.enrichment_id = p.enrichment_id AND e.source_id = p.source_id
WHERE p.parent_enrichment_id = in_enr.enrichment_id AND p.enrichment_id IS NOT NULL AND e.enrichment_id IS NULL;
IF v_error IS NOT NULL THEN
  RETURN format('Parameters %s reference enrichments that do not match enrichment_parameter.source_id. enrichment_id=%s',v_error, in_enr.enrichment_id);
END IF;

-- Validate that relation chain is valid
WITH ct AS (SELECT p.enrichment_parameter_id, meta.u_validate_relation_chain(in_enr.source_id, p.source_id, CASE WHEN p.aggregation_id IS NULL THEN '1' ELSE 'M' END, p.source_relation_ids) ch
FROM meta.enrichment_parameter p 
WHERE p.parent_enrichment_id = in_enr.enrichment_id AND COALESCE(p.source_relation_ids,'{}'::int[]) <> '{}'::int[]
)
SELECT string_agg(format('Parameter %s Error: %s',enrichment_parameter_id, ch),',') INTO v_error
FROM ct WHERE ch <> '';

IF v_error IS NOT NULL THEN
  RETURN format('Invalid relation chain: %s enrichment_id=%s',v_error, in_enr.enrichment_id);
END IF;

-- DONE with parameter validations
RETURN '';

END;

$BODY$;

-- validate that parameters and aggregates used in the expression match those in _params and _aggs tables
-- returns null when passed, otherwise returns error
CREATE OR REPLACE FUNCTION meta.u_validate_expression_parameters(in_expression_parsed text)
    RETURNS text
    LANGUAGE 'plpgsql'
AS $BODY$

DECLARE 
  v_aggregate_ids_check  int[];
  v_param_ids_check  int[];
  v_error text;
BEGIN

--Check that all aggregates use relation_ids
SELECT array_agg(a.id)::text INTO v_error
FROM _aggs a WHERE a.relation_ids IS NULL OR a.relation_ids = '{}'::int[];
IF v_error IS NOT NULL THEN
  RETURN format('Aggregations %s have blank relation_ids. expression: %s',v_error, in_expression_parsed);
END IF;

--Check that all parameters pointing to [This] source have blank relation_ids
SELECT array_agg(a.id)::text INTO v_error
FROM _aggs a WHERE a.relation_ids IS NULL OR a.relation_ids = '{}'::int[];
IF v_error IS NOT NULL THEN
  RETURN format('Aggregations %s have blank relation_ids. expression: %s',v_error, in_expression_parsed);
END IF;


--Check if all aggregates used in expression_parsed match passed aggregates
SELECT array_agg(m[1]::int) INTO v_aggregate_ids_check FROM regexp_matches(in_expression_parsed, 'A<(\d+)>', 'g') m;

SELECT array_agg(a.id)::text INTO v_error
FROM _aggs a WHERE NOT a.id = ANY(v_aggregate_ids_check);
IF v_error IS NOT NULL THEN
  RETURN format('Aggregations %s are not referenced in the parsed expression %s',v_error, in_expression_parsed);
END IF;

SELECT array_agg(p)::text INTO v_error
FROM unnest(v_aggregate_ids_check) p 
WHERE p NOT IN (SELECT id FROM _aggs);

IF v_error IS NOT NULL THEN
  RETURN format('Aggregations %s referenced in the parsed expression %s are not passed',v_error, in_expression_parsed);
END IF;

--Check if all parameters used in expression_parsed and aggregate expressions are passed
SELECT array_agg(id) INTO v_param_ids_check 
  FROM (
    SELECT m[1]::int id FROM regexp_matches(in_expression_parsed, 'P<(\d+)>', 'g') m  
    UNION ALL
    SELECT m[1]::int FROM _aggs a CROSS JOIN regexp_matches(a.expression_parsed, 'P<(\d+)>', 'g') m
  ) t;

SELECT array_agg(p.id)::text INTO v_error
FROM _params p WHERE NOT p.id = ANY(v_param_ids_check);
IF v_error IS NOT NULL THEN
  RETURN format('Parameters %s are not referenced in the parsed enrichment expression %s or aggregates',v_error, in_expression_parsed);
END IF;

SELECT array_agg(p)::text INTO v_error
FROM unnest(v_param_ids_check) p 
WHERE p NOT IN (SELECT id FROM _params);

IF v_error IS NOT NULL THEN
  RETURN format('Parameters %s referenced in the parsed expression %s or aggregate are not passed. Params: %s',v_error, in_expression_parsed, (select json_agg(p) FROM _params p));
END IF;
-- DONE with parameter validations

RETURN '';
END;

$BODY$;
DROP FUNCTION IF EXISTS meta.u_validate_output(in_output meta.output, in_import_mode text, OUT out_status character, OUT out_error text);
CREATE OR REPLACE FUNCTION meta.u_validate_output(in_output meta.output)
 RETURNS text
 LANGUAGE plpgsql
AS $function$

DECLARE
    v_output_id int;
    v_error json;
    v_original meta.output;
    v_source_names text[];
    v_bad_columns text;

BEGIN

    SELECT *
    INTO v_original
    FROM meta.output o
    WHERE output_id = in_output.output_id;

IF in_output.output_type = 'table' AND in_output.active_flag THEN
    SELECT json_build_object('output_name', o.output_name, 'project_name', p.name, 
    'table_name', in_output.output_package_parameters->>'table_name', 'table_schema',in_output.output_package_parameters->>'table_schema') 
    INTO v_error
    FROM meta.output o
    JOIN meta.project p ON o.project_id = p.project_id
    WHERE o.output_sub_type = in_output.output_sub_type
    AND o.output_type = in_output.output_type
    AND o.connection_id = in_output.connection_id
    AND output_package_parameters->>'table_name' = in_output.output_package_parameters->>'table_name'
    AND output_package_parameters->>'table_schema' = in_output.output_package_parameters->>'table_schema'
    AND output_id IS DISTINCT FROM in_output.output_id
    AND o.active_flag;
    IF v_error IS NOT NULL THEN
        RETURN format('An output writing to table already exists: %s',v_error);
    END IF;
END IF;

IF in_output.output_sub_type = 'text' AND (SELECT count(1) FROM meta.output_column WHERE output_id = in_output.output_id) > 1 THEN
     RETURN 'Text outputs can only have a single output column! Please remove excess columns or choose another output file type.';
END IF;

IF (in_output.output_type = 'file' AND (in_output.output_sub_type = 'parquet' OR in_output.output_sub_type = 'avro')) OR (in_output.output_type = 'table' AND in_output.output_sub_type = 'delta_lake') OR in_output.output_type = 'table'
    THEN
        SELECT string_agg(oc.name,',') INTO v_bad_columns
        FROM meta.output_column oc WHERE in_output.output_id = oc.output_id AND oc.name !~ ('^[a-zA-Z_]+[a-zA-Z0-9_]*$');
        IF v_bad_columns IS NOT NULL THEN
            IF in_output.output_type = 'table' THEN
                RETURN 'Output table type, column name must start with a letter and may contain letters, numbers, _ or spaces. column names: ' || v_bad_columns;
            ELSE
                RETURN 'Output types parquet, avro and delta lake cannot have spaces or special symbols in the column names: ' || v_bad_columns;
            END IF;
        END IF;
END IF;

IF in_output.output_type = 'virtual' AND in_output.output_package_parameters->>'view_name' IS NULL THEN
      RETURN 'Could not update Output ' || in_output.output_name || ' with a blank view name.';
END IF;

IF in_output.output_type = 'virtual' AND in_output.active_flag THEN
    SELECT json_build_object('output_name', o.output_name, 'project_name', p.name,
        'view_name',in_output.output_package_parameters->>'view_name', 'view_database', COALESCE(in_output.output_package_parameters->>'view_database',ip.schema_name) ) 
    INTO v_error
    FROM meta.output o
    JOIN meta.project p ON o.project_id = p.project_id
    JOIN meta.project ip ON in_output.project_id = ip.project_id
    WHERE o.output_type = in_output.output_type
    AND o.output_package_parameters->>'view_name' = in_output.output_package_parameters->>'view_name'
    AND COALESCE(o.output_package_parameters->>'view_database',p.schema_name) = COALESCE(in_output.output_package_parameters->>'view_database',ip.schema_name)
    AND output_id IS DISTINCT FROM in_output.output_id
    AND o.active_flag;

    IF v_error IS NOT NULL THEN
        RETURN format('An output writing to view already exists: %s',v_error);
    END IF;
END IF;


IF in_output.output_type <> 'virtual' AND v_original.output_type = 'virtual' THEN
    SELECT array_agg(s.source_name)
       INTO v_source_names
       FROM meta.source s WHERE s.loopback_output_id = in_output.output_id;
    IF v_source_names IS NOT NULL THEN
        RETURN 'Virtual output is referenced by loopback sources ' || v_source_names::text;
    END IF;
END IF;

RETURN '';

END;

$function$;


CREATE OR REPLACE FUNCTION meta.u_validate_output(in_output json)
 RETURNS text
 LANGUAGE plpgsql
AS $function$

DECLARE
    v_o meta.output;
BEGIN

SELECT * FROM json_populate_record(null::meta.output, in_output )  INTO v_o;

RETURN meta.u_validate_output(v_o);

END;

$function$;CREATE OR REPLACE FUNCTION meta.u_validate_output_mapping(in_om meta.output_source_column)
    RETURNS text
    LANGUAGE 'plpgsql'
AS $BODY$

DECLARE 
  v_output_name text;
  v_output_source_name text;
  v_column_name text;
  v_error text;
  v_source_id int;
  v_dest_source_id int;
BEGIN

SELECT os.source_id, os.output_source_name,o.output_name
INTO v_source_id,v_output_source_name,v_output_name
FROM meta.output_source os JOIN meta.output o ON o.output_id = os.output_id
WHERE os.output_source_id = in_om.output_source_id;

SELECT name INTO v_column_name
FROM meta.output_column oc 
WHERE oc.output_column_id = in_om.output_column_id;

IF v_source_id IS NULL THEN
  RETURN format('Unable to match source_id for output_source_id=%', in_om.output_source_id);
END IF;

IF in_om.type = 'raw' THEN
  IF in_om.raw_attribute_id IS NULL THEN
    RETURN format('raw_attribute_id is NULL. Output: %s Channel: %s Column: %s', v_output_name,v_output_source_name,v_column_name);
  END IF;

  SELECT source_id INTO v_dest_source_id
  FROM meta.raw_attribute r
  WHERE r.raw_attribute_id = in_om.raw_attribute_id;

  IF v_dest_source_id IS NULL THEN
    RETURN format('raw_attribute_id=%s does not exist. Output: %s Channel: %s Column: %s', in_om.raw_attribute_id, v_output_name,v_output_source_name,v_column_name);
  END IF;
ELSEIF in_om.type = 'enrichment' THEN
  IF in_om.enrichment_id IS NULL THEN
    RETURN format('enrichment_id is NULL. Output: %s Channel: %s Column: %s', v_output_name,v_output_source_name,v_column_name);
  END IF;

  SELECT source_id INTO v_dest_source_id
  FROM meta.enrichment e
  WHERE e.enrichment_id = in_om.enrichment_id;

  IF v_dest_source_id IS NULL THEN
    RETURN format('enrichment_id=% does not exist. Output: %s Channel: %s Column: %s', in_om.enrichment_id, v_output_name,v_output_source_name,v_column_name);
  END IF;
ELSEIF in_om.type = 'system' THEN
  IF in_om.system_attribute_id IS NULL THEN
    RETURN format('system_attribute_id is NULL. Output: %s Channel: %s Column: %s', v_output_name,v_output_source_name,v_column_name);
  END IF;

  v_dest_source_id := v_source_id;

  IF NOT EXISTS(SELECT 1 FROM meta.system_attribute WHERE system_attribute_id = in_om.system_attribute_id ) THEN
    RETURN format('system_attribute_id=% does not exist. Output: %s Channel: %s Column: %s', in_om.system_attribute_id, v_output_name,v_output_source_name,v_column_name);
  END IF;
ELSE
  RETURN format('Invalid attribute type %s. Output: %s Channel: %s Column: %s', COALESCE(in_om.type,'NULL'), v_output_name,v_output_source_name,v_column_name);
END IF;


--Check that all parameters pointing to another source have non-blank relation_ids
IF v_dest_source_id <> v_source_id THEN
  IF COALESCE(in_om.source_relation_ids,'{}'::int[]) = '{}'::int[] THEN
    RETURN format('Related source parameter %s has blank source_relation_ids.  Output: %s Channel: %s Column: %s', v_output_name,v_output_source_name,v_column_name);
  END IF;
  v_error := meta.u_validate_relation_chain(v_source_id, v_dest_source_id, '1', in_om.source_relation_ids);
  IF v_error <> '' THEN
    RETURN format('%s Output: %s Channel: %s Column: %s',  v_error, v_output_name,v_output_source_name,v_column_name);
  END IF;
END IF;


-- DONE with validations
RETURN '';

END;

$BODY$;


-- validate that the relation connects 2 sources
CREATE OR REPLACE FUNCTION meta.u_validate_relation_chain(in_from_source_id int, in_to_source_id int, 
in_cardinality text, in_path int[])
 RETURNS text
 LANGUAGE plpgsql
AS $function$

DECLARE
	v_missing_relation_ids int[];
	v_max_length int = cardinality(in_path);

BEGIN

IF COALESCE(in_path,'{}'::int[]) = '{}'::int[] THEN
	RETURN 'Relation path is blank';
END IF;

-- check if relations in in_path exist and are active
SELECT array_agg(r.id) INTO v_missing_relation_ids
FROM unnest(in_path) r(id)
LEFT JOIN meta.source_relation sr ON sr.source_relation_id = r.id AND sr.active_flag
WHERE sr.source_relation_id IS NULL;

IF v_missing_relation_ids IS NOT NULL THEN
	RETURN format('Relation id(s) do not exist or are not active: %s',v_missing_relation_ids);
END IF;


IF NOT EXISTS(
	WITH recursive ct AS (
		SELECT ca.next_source_id, ca.cardinality,
		ARRAY[ca.source_relation_id] relation_ids, 1 path_level
			FROM meta.u_relation_with_cardinality(in_from_source_id) ca 
			WHERE ca.source_relation_id = in_path[1]
			
		UNION

		SELECT ca.next_source_id, ca.cardinality,
		ct.relation_ids || ca.source_relation_id relation_ids, ct.path_level + 1
			FROM ct CROSS JOIN meta.u_relation_with_cardinality(ct.next_source_id) ca
			WHERE ct.cardinality = '1' -- all relations prior to last one in the chain must be 1
			AND ca.source_relation_id = in_path[ct.path_level + 1]
			AND ct.path_level < v_max_length 
	)
	SELECT 1
	FROM ct
	WHERE next_source_id = in_to_source_id AND relation_ids = in_path AND cardinality = in_cardinality
) THEN
	RETURN format('Relation chain %s does not connect source_id=%s to source_id=%s with cardinality %s',
		in_path, in_from_source_id , in_to_source_id , in_cardinality) ;
END IF;

RETURN '';

END;

$function$;

