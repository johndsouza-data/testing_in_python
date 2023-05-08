"""

main module

"""

from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
import pyspark
from utils.utils import vaccum_columns

spark = pyspark.sql.SparkSession \
        .builder \
        .enableHiveSupport() \
        .getOrCreate()

deptColumns = ["dept_name", "dept_id", "dept_budget"]
deptColumns = StructType([
    StructField('rownumber', IntegerType(), True),
    StructField('name', StringType(), True)
])

schema = StructType([
    StructField('dept_id', IntegerType(), True),
    StructField('dept_name', StringType(), True),
    StructField('dept_head', StringType(), True),
])

dept = [(10, "Finance", None),
        (20, "Marketing", None),
        (30, "Sales", None),
        (40, "IT", None)
        ]

df: DataFrame = spark.createDataFrame(data=dept, schema=schema)
df.show()
df_clean = vaccum_columns(df)
df_clean.show()
