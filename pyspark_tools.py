import pandas as pd
import os
import pyspark.sql


def python_location():
    """work out the location of the python interpretter - this is needed for Pyspark to initialise"""
    import subprocess
    import pathlib
    with subprocess.Popen("where python", shell=True, stdout=subprocess.PIPE) as subprocess_return:
        python_exe_paths = subprocess_return.stdout.read().decode('utf-8').split('\r\n')
        env_path = pathlib.Path([x for x in python_exe_paths if '\\envs\\' in x][0])
    return str(env_path)


def initialise_spark():
    """This function creates a spark session if one doesn't already exist (i.e. within databricks this will do nothing)"""
    if 'spark' not in locals():
        os.environ["PYSPARK_PYTHON"] = python_location()
        spark = pyspark.sql.SparkSession.builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
    return spark


class getData:
    """this class acts as a middle man between the ETL processes and wherever the data is being stored.
    Iniially this will just be locally in CSV files"""
    def __init__(self, source='local', data_format='csv', spark_session=initialise_spark()):
        self.source = source
        self.format = data_format
        self.spark = spark_session

    def table(self, table_name):
        """Extract the 'table_name' data from the 'source' (where it is stored in the specified 'format'"""
        if self.source=='local' and self.format == 'csv':
            pdf_table = pd.read_csv(table_name)
            sdf_table = self.spark.createDataFrame(pdf_table)

        return sdf_table

