DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'parameter_map') THEN
        DROP TYPE parameter_map CASCADE; 
    END IF;
    CREATE TYPE parameter_map  AS (name text, type text, raw_attribute_id int, enrichment_id int, system_attribute_id int, source_id int, error text, datatype text, datatype_schema jsonb);
END$$;