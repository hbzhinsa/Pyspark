#%%
import os 
os.environ["JAVA_HOME"] = "/usr/local/opt/openjdk@11"
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Practise').getOrCreate()
spark
df_spark=spark.read.option("sep", ";").option("header", "true").csv('data.csv')
df_spark=spark.read.csv('data.csv',sep=";", header=True)
#%%
df_spark.printSchema()
# %%
df_spark.columns
# %%
df_spark.head(3)
# %%
df_spark.select('name').show()
# %%
df_spark.select(['name','times']).show()
# %%
df_spark.dtypes


# %%
df_spark.describe().show()

# %%
df_spark.withColumn
df_spark.dorp('id')
# %%
df_spark.withColumnRenamed('name','Name').show()
# %%
