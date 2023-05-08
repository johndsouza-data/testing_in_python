"""

Unit test module to validate the main.utils module

"""

import pytest

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from utils.utils import vaccum_columns

@pytest.mark.usefixtures('spark_test_ses')
def test_vacuum_columns_df_with_empty_column(spark_test_ses):
    """vacuum_columns(DatFrame): Vacuums the columns from the DataFrame when all the cells are empty."""
    schema = StructType([StructField('rownumber', IntegerType(), True), StructField('name', StringType(), True)])
    df: DataFrame = spark_test_ses.createDataFrame(data=[(1, None), (1, None), (3, None), (4, None), (5, None)],
                                                   schema=schema)
    expected_df = df.drop('name')
    # Scenario: Output DF is not empty as one of the columns is non-empty.
    output_df: DataFrame = vaccum_columns(df)
    assert not output_df.rdd.isEmpty()

    # Scenario: The empty column is vacuumed from the DataFrame
    assert len(output_df.columns) == 1
    assert output_df.columns == ['rownumber']
    # Scenario: The output_df should match the expected_df dataframe
    # assert expected_df.intersectAll(output_df).count() == 5

@pytest.mark.usefixtures('spark_test_ses')
def test_vacuum_columns_df_with_atleast_one_non_empty_cell(spark_test_ses):
    """vaccum_columns(DataFrame): Doesn't vacuum the column from the DataFrame when atleast one cell is non-empty."""
    schema = StructType([StructField('id', IntegerType(), True), StructField('file_name', StringType(), True)])
    df: DataFrame = spark_test_ses.createDataFrame(data=[(1, "a.parquet"), (1, None), (3, None), (4, None), (5, None)],
                                                   schema=schema)
    expected_df = df

    # Scenario: Output DF is not empty as one of the columns is non-empty.
    output_df: DataFrame = vaccum_columns(df)
    assert not output_df.rdd.isEmpty()

    # Scenario: The empty column is vacuumed from the DataFrame
    assert len(output_df.columns) == 2
    assert output_df.columns == ['id', 'file_name']

    # Scenario: The output_df should match the expected_df dataframe
    # assert expected_df.intersectAll(output_df).count() == 5

