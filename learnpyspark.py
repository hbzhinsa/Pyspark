#%%
import os 
os.environ["JAVA_HOME"] = "/usr/local/opt/openjdk@11"
import pyspark
from pyspark.sql import SparkSession
import pandas as pd
# %%
pd.read_csv('data.csv')

# %%
spark=SparkSession.builder.appName('Practise').getOrCreate()
# %%
df_pyspark=spark.read.csv('data.csv') 
# %%
df_pyspark.show()
###
spark.read.option("sep", ";").option("header", "true").csv('data.csv').show()
# %%
type(df_pyspark)
# %%
df_pyspark.printSchema()
# %%
