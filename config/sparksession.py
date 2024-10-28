import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Assignment")\
    .config("spark.jars", "/mnt/c/Nteq-Assignment/config/spark_jars/snowflake-jdbc-3.13.30.jar,/mnt/c/Nteq-Assignment/config/spark_jars/spark-snowflake_2.12-2.15.0-spark_3.4.jar")\
    .getOrCreate()