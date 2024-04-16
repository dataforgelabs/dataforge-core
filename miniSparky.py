import json

from pyspark.sql import SparkSession
from pyspark.sql.types import DataType, StructType, ArrayType, DecimalType
from mainConfig import MainConfig


class MiniSparky:
    def __init__(self, config: MainConfig):
        self._config = config
        self.spark = SparkSession.builder \
            .appName("dfCore") \
            .master("local[1]") \
            .config("spark.log.level", "ERROR") \
            .config("spark.driver.memory", "512m") \
            .config("spark.executor.memory", "512m") \
            .config("spark.executor.cores", "1") \
            .config("spark.ui.enabled", "false") \
            .config("spark.driver.host", "localhost") \
            .getOrCreate()

        self.spark.sql(
            """SELECT CAST(-87.68 as DECIMAL(10,2)) `decimal` , CAST(13518864 as BIGINT) `bigint`, CAST('Western Ave & Walton St' as STRING) `string`, CAST(130 AS INT) `int`, CAST(130 AS INT) `integer`, CAST(41.90331 as FLOAT) `float` , CAST(87.67695 as DOUBLE) `double`, CAST('2017-03-31' as DATE) `date`, CAST('2017-03-31T23:19:17.000+0000' as TIMESTAMP) `timestamp` , true `boolean`, CAST(9999999999 as BIGINT) `long`
                UNION ALL
                SELECT CAST(-8127.68 as DECIMAL(10,2)) `decimal` , CAST(1518864 as BIGINT) `bigint`, CAST('Western Ave & Walton St' as STRING) `string`, CAST(130 AS INT) `int`, CAST(130 AS INT) `integer`, CAST(41.90331 as FLOAT) `float` , CAST(87.67695 as DOUBLE) `double`, CAST('2020-03-31' as DATE) `date`, CAST('2020-03-31T23:19:17' as TIMESTAMP) `timestamp` , true `boolean`, CAST(99999999991 as BIGINT) `long`
                """).createOrReplaceTempView("datatypes")

        _res = self._config.pg.sql("select meta.svc_select_attribute_types_spark_to_hive()")
        self.type_map = {}
        for x in _res:
            self.type_map[x['spark_type']] = x['hive_type']

    @staticmethod
    def get_spark_type(spark_type: DataType) -> str:
        match spark_type:
            case StructType(_):
                return "StructType"
            case ArrayType(_, _):
                return "ArrayType"
            case DecimalType():
                return "DecimalType"
            case _:
                return str(spark_type).rstrip("()")

    def execute_query(self, query: str):
        try:
            df = self.spark.sql(query)
            field = df.schema.fields[0]
            is_null = df.head()[0] is None
            col_name = field.name
            dt = field.dataType
            spark_type = self.get_spark_type(dt)
            att_schema = field.dataType.json()
            data_type = self.type_map.get(spark_type)

            if is_null:
                return {
                    "type": "warning",
                    "data_type": data_type,
                    "att_schema": att_schema,
                    "message": "NULL values detected! This typically indicates improper type casting or that you're doing some very complex logic"
                }
            elif col_name != "col1":
                return {
                    "type": "error",
                    "message": "Extraneous input detected at end of expression"
                }
            else:
                return {
                    "type": "success",
                    "data_type": data_type,
                    "att_schema": att_schema
                }
        except Exception as e:
            print("spark exception, printing stack trace")
            return {
                "type": "error",
                "message": str(e)
            }
