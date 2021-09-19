#%%
import os 
os.environ["JAVA_HOME"] = "/usr/local/opt/openjdk@11"
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Practise').getOrCreate()
spark
df_spark=spark.read.option("sep", ";").option("header", "true").csv('data.csv')
df_spark=spark.read.csv('data2.csv',sep=";", header=True, inferSchema=True)

# %%
df_spark.filter('times>10').select('name').show()
# %%
df_spark.filter(df_spark['times']>10).show()
# %%
