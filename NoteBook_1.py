# Databricks notebook source
# DBTITLE 1,calculate_percentile
sampleData1 = [("bob","Developer",125000),("mark","Developer",108000),("carl","Tester",70000),("peter","Developer",185000),("jon","Tester",65000),("roman","Tester",82000),("simon","Developer",98000),("eric","Developer",144000),("carlos","Tester",75000),("henry","Developer",110000),("Manjunath","Developer",10000)]

it_df1 = spark.createDataFrame(sampleData1,schema=["Name","Role","Salary"])
it_df1.orderBy('Role','salary').show()

# COMMAND ----------

it_df1.createOrReplaceTempView('test')
itdf2 = spark.sql('select role, percentile(salary,array(0.25,0.5,0.75),100) as quantiles from test group by Role ')
itdf2.show(10,False)

# COMMAND ----------

itdf2.select('role', itdf2.quantiles[0].alias('25_percentile'), itdf2.quantiles[1].alias('50_percentile'), itdf2.quantiles[2].alias('75_percentile')).show()

# COMMAND ----------

spark.conf.set("spark.scheduler.mode","FAIR")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, explode
from pyspark.sql import functions as F
import numpy as np
import concurrent.futures
import time


k_ft = 0.02

def create_dataframe(spark_sess):
    data = [{'ls': 3, 'track_type': 'M', 'side_track_no': '1', 'MBMP': 503.718, 'MEMP': 505.293},
            {'ls': 3, 'track_type': 'M', 'side_track_no': '1', 'MBMP': 507.718, 'MEMP': 509.293},
            {'ls': 3, 'track_type': 'M', 'side_track_no': '1', 'MBMP': 510.718, 'MEMP': 512.293},
            {'ls': 3, 'track_type': 'M', 'side_track_no': '1', 'MBMP': 514.718, 'MEMP': 516.293}]
    return spark_sess.createDataFrame(data)
  
def truncate(n):
    return int(n * 10000) / 10000


def create_chunks(major_BMP,major_EMP):
    global k_ft
    chunks_list = np.arange(major_BMP, major_EMP, k_ft)
    chunks = []
    for i in chunks_list:
        j = truncate(i)
        chunks.append([j, truncate(j + k_ft)])
    return chunks
  
def process_dataframe(input_df):
    input_df = input_df.withColumn("chunk_list", create_chunks_udf('MBMP', 'MEMP'))
    input_df = input_df.withColumn("chunks", explode(F.col('chunk_list')))
    data_df1 = input_df.withColumn("minor_bmp", input_df.chunks[0]).withColumn("minor_emp", input_df.chunks[1])
    data_df2 = data_df1.drop("chunk_list", "chunks", "MBMP", "MEMP")
    return data_df2
  
if __name__ == '__main__':
    start = time.perf_counter()
    #spark = SparkSession.builder.master('local[*]').enableHiveSupport().getOrCreate()
    data_df = create_dataframe(spark)
    data_df.show()
    #df1, df2 = data_df.randomSplit([0.5, 0.5])
    #print('count of df1:{} and df2:{}'.format(df1.count(), df2.count()))
    create_chunks_udf = udf(lambda x, y: create_chunks(x, y), ArrayType(ArrayType(FloatType())))
    
    with concurrent.futures.ProcessPoolExecutor(1) as executor:
        datasets = [data_df,data_df,data_df,data_df,data_df,data_df,data_df,data_df,data_df,data_df]
        results = executor.map(process_dataframe, datasets)
        for f in results:
            print(f.count())
            

    #print('the final total no of rows are:{}'.format(final_df.count()))
    finish = time.perf_counter()
    print(f'Finished in {round(finish - start, 2)}, seconds(s)')  

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, explode
from pyspark.sql import functions as F
import numpy as np
import concurrent.futures
import time


k_ft = 0.02

def create_dataframe(spark_sess):
    data = [{'ls': 3, 'track_type': 'M', 'side_track_no': '1', 'MBMP': 503.718, 'MEMP': 505.293},
            {'ls': 3, 'track_type': 'M', 'side_track_no': '1', 'MBMP': 507.718, 'MEMP': 509.293},
            {'ls': 3, 'track_type': 'M', 'side_track_no': '1', 'MBMP': 510.718, 'MEMP': 512.293},
            {'ls': 3, 'track_type': 'M', 'side_track_no': '1', 'MBMP': 514.718, 'MEMP': 516.293}]
    return spark_sess.createDataFrame(data)
  
def truncate(n):
    return int(n * 10000) / 10000


def create_chunks(major_BMP,major_EMP):
    global k_ft
    chunks_list = np.arange(major_BMP, major_EMP, k_ft)
    chunks = []
    for i in chunks_list:
        j = truncate(i)
        chunks.append([j, truncate(j + k_ft)])
    return chunks
  
def process_dataframe(input_df):
    input_df = input_df.withColumn("chunk_list", create_chunks_udf('MBMP', 'MEMP'))
    input_df = input_df.withColumn("chunks", explode(F.col('chunk_list')))
    data_df1 = input_df.withColumn("minor_bmp", input_df.chunks[0]).withColumn("minor_emp", input_df.chunks[1])
    data_df2 = data_df1.drop("chunk_list", "chunks", "MBMP", "MEMP")
    return data_df2
  
if __name__ == '__main__':
    start = time.perf_counter()
    spark = SparkSession.builder.master('local[*]').enableHiveSupport().getOrCreate()
    data_df = create_dataframe(spark)
    data_df.show()
    create_chunks_udf = udf(lambda x, y: create_chunks(x, y), ArrayType(ArrayType(FloatType())))
    
    for _ in range(10):
      res_df = process_dataframe(data_df)
      print(res_df.count())

    #print('the final total no of rows are:{}'.format(final_df.count()))
    finish = time.perf_counter()
    print(f'Finished in {round(finish - start, 2)}, seconds(s)')  

# COMMAND ----------

# MAGIC %cmd
# MAGIC databricks secrets list --scope uppa_scope

# COMMAND ----------

# MAGIC %fs

# COMMAND ----------

dbutils.secrets.list('')

# COMMAND ----------

import pyodbc

# COMMAND ----------

SQL_SERVER_USER = dbutils.secrets.get('uppa_scope', 'SQL_SERVER_USER')
SQL_SERVER_PASSWORD  = dbutils.secrets.get('uppa_scope', 'SQL_SERVER_PASSWORD')
SQL_SERVER_HOST = dbutils.secrets.get('uppa_scope', 'SQL_SERVER_HOST')
SQL_SERVER_PORT = dbutils.secrets.get('uppa_scope', 'SQL_SERVER_PORT')
SQL_SERVER_DATABASE = dbutils.secrets.get('uppa_scope', 'SQL_SERVER_DATABASE')

print(SQL_SERVER_USER, SQL_SERVER_PASSWORD, SQL_SERVER_HOST, SQL_SERVER_PORT, SQL_SERVER_DATABASE)
    

# COMMAND ----------

database = "test"
table = "dbo.Employees"
user = "zeppelin"
password  = "zeppelin"

conn = pyodbc.connect(f'DRIVER={{ODBC Driver 13 for SQL Server}};SERVER=localhost,1433;DATABASE={database};UID={user};PWD={password}')
query = f"SELECT EmployeeID, EmployeeName, Position FROM {table}"
pdf = pd.read_sql(query, conn)
sparkDF =  spark.createDataFrame(pdf)
sparkDF.show()

# COMMAND ----------

dbutils.secrets.get('uppa_scope','SQL_SERVER_DATABASE')

# COMMAND ----------

# MAGIC %run /Users/manjunath.swamy@bnsf.com/UPPA_TEST/UnifiedDataPipeline_test/python/config

# COMMAND ----------

conf = config()

# COMMAND ----------

credentials_list = (conf.getSQL_SERVER_CREDENTIALS())
user = credentials_list[0]
password = credentials_list[1]
host = credentials_list[2]
port = credentials_list[3]
database = credentials_list[4]

# COMMAND ----------

conn_string = 'DRIVER={};SERVER=uppa-sql.database.windows.net;DATABASE={};UID={};PWD={}'.format("{ODBC Driver 17 for SQL Server}",database,user_name,passwd)

    conn = pyodbc.connect(conn_string)
    conn.autocommit=True
    cursor = conn.cursor()

# COMMAND ----------

# DBTITLE 1,connecting sql server and reading table as dataframe
import pandas as pd
conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=uppa-sql.database.windows.net,1433;DATABASE={database};UID={user};PWD={password}')
query = "select MAJOR_TRACK_SEGMENT_ID, ANOMALY_THRESHOLD from UPPA.MAJOR_TRACK_SEGMENT"
pdf = pd.read_sql(query, conn)
sparkDF =  spark.createDataFrame(pdf)
sparkDF.count()

# COMMAND ----------

df = spark.read.format('jdbc').option('url',f'jdbc:sqlserver://{host}:{port}/databaseName='UPPA';').option('user',{user}).option('password',{password}).option('dbtable',"select * from UPPA.MAJOR_TRACK_SEGMENT")

# COMMAND ----------

df.show()

# COMMAND ----------

py4j.protocol.Py4JJavaError: An error occurred while calling o25.get.
: java.lang.SecurityException: Accessing a secret from Databricks Connect requires
a privileged secrets token. To obtain such a token, you can run the following
in a Databricks workspace notebook:

displayHTML(
  "<b>Privileged DBUtils token (expires in 48 hours): </b>" +
  dbutils.notebook.getContext.apiToken.get.split("").mkString("<span/>"))

Then, run dbutils.secrets.setToken(<value>) locally to save the token.
Note that these tokens expire after **48 hours**.

	at com.databricks.service.SecretUtils$.getPrivilegedToken(DBUtils.scala:96)
	at com.databricks.service.SecretUtils$.getBytes(DBUtils.scala:120)
	at com.databricks.service.SecretUtils$.get(DBUtils.scala:116)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(Unknown Source)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(Unknown Source)
	at java.lang.reflect.Method.invoke(Unknown Source)
	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:380)
	at py4j.Gateway.invoke(Gateway.java:295)
	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
	at py4j.commands.CallCommand.execute(CallCommand.java:79)
	at py4j.GatewayConnection.run(GatewayConnection.java:251)
	at java.lang.Thread.run(Unknown Source)


Process finished with exit code 1


# COMMAND ----------

# MAGIC %scala
# MAGIC displayHTML(
# MAGIC   "<b>Privileged DBUtils token (expires in 48 hours): </b>" +
# MAGIC   dbutils.notebook.getContext.apiToken.get.split("").mkString("<span/>"))

# COMMAND ----------


dbutils.notebook.getContext.apiToken.get.split("").mkString("<span/>")

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.notebook.

# COMMAND ----------

# MAGIC %sh
# MAGIC python --version

# COMMAND ----------

