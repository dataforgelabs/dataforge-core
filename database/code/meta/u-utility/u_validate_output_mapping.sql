CREATE OR REPLACE FUNCTION meta.u_validate_output_mapping(in_om meta.output_source_column)
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
    RETURN format('Related source parameter %s has blank source_relation_ids.  Output: %s Channel: %s Column: %s Attribute Source Name: %s Source_id: %s Channel Source Name: %s Channel Source_id: %s', 
    in_om, v_output_name,v_output_source_name,v_column_name, meta.u_get_source_name(v_dest_source_id),v_dest_source_id,  meta.u_get_source_name(v_source_id), v_source_id);
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

