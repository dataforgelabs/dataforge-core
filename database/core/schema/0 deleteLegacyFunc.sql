--drop legacy functions
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
END $$;