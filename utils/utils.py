"""

utils


"""

# import findspark
# findspark.init("/opt/spark")

# from typing import Union
# from collections import defaultdict
# from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when

def vaccum_columns(f_df):
    """

    :param df:
    :return:
    This function drops columns containing null values only
    if in every cell in that column is blank/null/invalid
    """
    null_counts = f_df.select([count(when(col(c).isNull(), c)).alias(c)
                             for c in f_df.columns]).collect()[0].asDict()
    to_drop = [k for k, v in null_counts.items() if v >= f_df.count()]
    f_df = f_df.drop(*to_drop)
    return f_df
