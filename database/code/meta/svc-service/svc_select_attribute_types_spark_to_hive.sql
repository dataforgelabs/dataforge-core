CREATE OR REPLACE FUNCTION meta.svc_select_attribute_types_spark_to_hive()
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
$function$;