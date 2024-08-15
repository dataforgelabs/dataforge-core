-- build constant expression for testing parameter data type 
-- This wrapper function is called from the outside world
CREATE OR REPLACE FUNCTION meta.u_datatype_test_expression(in_datatype text, in_datatype_schema jsonb)
    RETURNS text
    LANGUAGE 'plpgsql'
AS
$BODY$

BEGIN
    RETURN meta.u_datatype_test_expression_json(in_datatype, in_datatype_schema::json);
END;
$BODY$;


-- This function is internal / recursive. It requires json type for in_datatype_schema in order to preserve ordering of nested struct fields
CREATE OR REPLACE FUNCTION meta.u_datatype_test_expression_json(in_datatype text, in_datatype_schema json)
    RETURNS text
    LANGUAGE 'plpgsql'

AS
$BODY$

DECLARE
    v_fields json;
    v_array_type json;
    v_array_sub_exp text;
    v_exp text;
BEGIN

IF in_datatype = 'struct' THEN 
    PERFORM meta.u_assert(in_datatype_schema->>'type' = 'struct',format('datatype_schema %s does not match datatype %s',in_datatype_schema,in_datatype));
    v_fields := in_datatype_schema->'fields';
    PERFORM meta.u_assert(json_typeof(v_fields) = 'array' ,format('Invalid fields value in datatype_schema %s for datatype %s',in_datatype_schema,in_datatype));

    -- Create expression as struct( field1.typeExp AS field1.name, field2.typeExp AS field2.name, ... )

    SELECT 'struct(' || string_agg(CASE WHEN json_typeof(field->'type') = 'string' THEN format('%s AS `%s`',meta.u_datatype_test_expression_json(field->>'type', null), field->>'name') 
        WHEN json_typeof(field->'type') = 'object' THEN format('%s AS `%s`',meta.u_datatype_test_expression_json(field->'type'->>'type', field->'type'), field->>'name') 
        ELSE format('ERROR: Invalid type %s of field %s',field->>'type', field->>'name') END,', ') || ')'
    INTO v_exp
    FROM json_array_elements(v_fields) field;

    PERFORM meta.u_assert(v_exp IS NOT NULL ,format('Null expression in datatype_schema %s for datatype %s',in_datatype_schema,in_datatype));
    PERFORM meta.u_assert(v_exp NOT LIKE 'ERROR: Invalid type %' ,format('Error in expression %s in datatype_schema %s for datatype %s',v_exp ,in_datatype_schema,in_datatype));

ELSEIF in_datatype = 'array' THEN 
    PERFORM meta.u_assert(in_datatype_schema->>'type' = 'array',format('datatype_schema %s does not match datatype %s',in_datatype_schema,in_datatype));
    v_array_type := in_datatype_schema->'elementType';

    -- Create expression as array( element1, element2 )
    IF json_typeof(in_datatype_schema->'elementType') = 'string' THEN -- array of simple type
        v_array_sub_exp := meta.u_datatype_test_expression_json(in_datatype_schema->>'elementType', null); 
    ELSEIF json_typeof(in_datatype_schema->'elementType') = 'object' THEN -- nested array
        v_array_sub_exp := meta.u_datatype_test_expression_json(in_datatype_schema->'elementType'->>'type', in_datatype_schema->'elementType');
    ELSE
        PERFORM meta.u_assert(false, format('Invalid array elementType: %s'),in_datatype_schema->>'elementType');
    END IF;

    v_exp := format('array(%s,%s)',v_array_sub_exp,v_array_sub_exp);

ELSEIF in_datatype like 'decimal(%' THEN 
    v_exp :=  format('CAST(`decimal` AS decimal(38,12))',in_datatype);
ELSE
   v_exp :=  '`' || in_datatype || '`';
END IF;   

RETURN v_exp;

END;
$BODY$;

