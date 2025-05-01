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
CREATE OR REPLACE FUNCTION meta.u_validate_expression_parameters(in_expression_parsed text, in_source_id int)
    RETURNS text
    LANGUAGE 'plpgsql'
AS $BODY$

DECLARE 
  v_aggregate_ids_check  int[];
  v_param_ids_check  int[];
  v_error text;
BEGIN

IF EXISTS(SELECT 1 FROM meta.source WHERE source_id = in_source_id AND processing_type = 'stream') THEN
    SELECT array_agg(p.id)::text INTO v_error
    FROM _params p 
    WHERE aggregation_id IS NOT NULL;
    IF v_error IS NOT NULL THEN
      RETURN format('Aggregates are not supported in stream source rules. Parameters: %s Expression: %s',v_error, in_expression_parsed);
    END IF;
END IF;


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