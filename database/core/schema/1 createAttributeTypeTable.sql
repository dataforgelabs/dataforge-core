DROP TABLE IF EXISTS  meta.attribute_type CASCADE;
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
('date','date','{DateType}',false);