# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC drop database demo_db cascade;

# COMMAND ----------

from pyspark.sql import Row
l=[(2001, "Iphone12", 11, "I"), (2002, "One plus7", 33, "I"), (2003, "Redminote10", 41, "I"), (2004, "Samsung S10", 14, "I")]
rdd = sc.parallelize(l)
schema_rdd=rdd.map(lambda x : Row(product_id = int(x[0]), product_name = x[1], stocks_left = int(x[2]), opn_flag=x[3] ))
df=spark.createDataFrame(schema_rdd)
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create database demo_db;

# COMMAND ----------

database_name = 'demo_db'
delta_table_name = 'product_delta_table'
parquet_table_name = "product_parquet_table"

# COMMAND ----------

# DBTITLE 1,Initial Load For Parquet Format
(df.coalesce(2).write.format("parquet").mode("overwrite").saveAsTable(f'{database_name}.{parquet_table_name}'))


# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/demo_db.db/product_parquet_table

# COMMAND ----------

(df.coalesce(2).write.format("parquet").mode("overwrite").saveAsTable(f'{database_name}.{parquet_table_name}'))

# COMMAND ----------

# DBTITLE 1,Existing Data Overridden (Parquet Format)
# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/demo_db.db/product_parquet_table

# COMMAND ----------

# DBTITLE 1,Initial Load For Delta Format
(df.coalesce(2).write.format("delta").mode("overwrite").saveAsTable(f'{database_name}.{delta_table_name}'))

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/demo_db.db/product_delta_table

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/user/hive/warehouse/demo_db.db/product_delta_table/_delta_log

# COMMAND ----------

# DBTITLE 1,Versioned Parquet files to store Data
(df.coalesce(2).write.format("delta").mode("overwrite").saveAsTable(f'{database_name}.{delta_table_name}'))

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/demo_db.db/product_delta_table

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/demo_db.db/product_delta_table/_delta_log

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC head dbfs:/user/hive/warehouse/demo_db.db/product_delta_table/_delta_log/00000000000000000000.json

# COMMAND ----------

display(spark.read.json('dbfs:/user/hive/warehouse/demo_db.db/product_delta_table/_delta_log/00000000000000000000.json'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_db.product_delta_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_db.product_parquet_table

# COMMAND ----------

l=[(2001, "Iphone12", "eleven", "I"), (2002, "One plus7", 33.22, "I"), (2003, "Redminote10", 41, "I"), (2004, "Samsung S10", 14, "I")]
rdd = sc.parallelize(l)
schema_Rdd=rdd.map(lambda x : Row(product_id = int(x[0]), product_name = x[1], stocks_left = str(x[2]), opn_flag=x[3] ))
type_changed_df=spark.createDataFrame(schema_Rdd)
display(type_changed_df)

# COMMAND ----------

type_changed_df.printSchema()

# COMMAND ----------

# DBTITLE 1,SCHEMA ENFORCEMENT
(type_changed_df.repartition(2).write.format("parquet").mode("overwrite").saveAsTable(f'{database_name}.{parquet_table_name}'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_db.product_parquet_table

# COMMAND ----------

(type_changed_df.repartition(2).write.format("delta").mode("overwrite").saveAsTable(f'{database_name}.{delta_table_name}'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_db.product_delta_table

# COMMAND ----------

l=[(2001, "Iphone12", 11, "I", "new"), (2002, "One plus7", 33, "I", "old"), (2003, "Redminote10", 41, "I", "new"), (2004, "Samsung S10", 14, "I", "old")]
rdd = sc.parallelize(l)
schema_Rdd=rdd.map(lambda x : Row(product_id = int(x[0]), product_name = x[1], stocks_left = (x[2]), opn_flag=x[3], new_col=x[4] ))
newcol_df=spark.createDataFrame(schema_Rdd)
display(newcol_df)

# COMMAND ----------

# DBTITLE 1,SCHEMA EVOLUTION
(newcol_df.repartition(2).write.format("parquet").mode("append").option("mergeSchema", True).saveAsTable(f'{database_name}.{parquet_table_name}'))

# COMMAND ----------

(newcol_df.repartition(2).write.format("delta").mode("append").saveAsTable(f'{database_name}.{delta_table_name}'))

# COMMAND ----------

(newcol_df.repartition(2).write.format("delta").mode("append").option("mergeSchema", True).saveAsTable(f'{database_name}.{delta_table_name}'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_db.product_delta_table

# COMMAND ----------

# DBTITLE 1,CDC MERGE USECASE
l=[(2001, "Iphone12", 10, "U", "new"), (2002, "One plus7", 12, "U", "old"), (2003, "Redminote10", 20, "U", "new"), (2004, "Samsung S10", 0, "D", "old")]
rdd = sc.parallelize(l)
schema_Rdd=rdd.map(lambda x : Row(product_id = int(x[0]), product_name = x[1], stocks_left = (x[2]), opn_flag=x[3], new_col=x[4] ))
merge_df=spark.createDataFrame(schema_Rdd)
display(merge_df)

# COMMAND ----------

merge_df.createOrReplaceTempView('source_table')

# COMMAND ----------

spark.sql("""
          MERGE INTO demo_db.product_delta_table target_table
          USING source_table
          ON source_table.product_id = target_table.product_id
          WHEN MATCHED AND source_table.opn_flag = 'U'
          THEN
          UPDATE SET *
          WHEN MATCHED AND source_table.opn_flag = 'D'
          THEN DELETE
          WHEN NOT MATCHED and source_table.opn_flag = 'I'
          THEN INSERT *
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_db.product_delta_table

# COMMAND ----------

# DBTITLE 1,Inserts, Updates, deletes count
# MAGIC %sql
# MAGIC 
# MAGIC describe history demo_db.product_delta_table

# COMMAND ----------

# DBTITLE 1,Time Travel
# MAGIC %sql
# MAGIC 
# MAGIC select * from demo_db.product_delta_table version as of 0

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from demo_db.product_delta_table timestamp as of '2020-12-24T08:24:49.000+0000'

# COMMAND ----------

# DBTITLE 1,TRANSACTIONAL LOG FILES
# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/user/hive/warehouse/demo_db.db/product_delta_table/_delta_log

# COMMAND ----------

display(spark.read.json('dbfs:/user/hive/warehouse/demo_db.db/product_delta_table/_delta_log/00000000000000000000.json'))

# COMMAND ----------

display(spark.read.json('dbfs:/user/hive/warehouse/demo_db.db/product_delta_table/_delta_log/00000000000000000002.json'))

# COMMAND ----------

display(spark.read.json('dbfs:/user/hive/warehouse/demo_db.db/product_delta_table/_delta_log/00000000000000000003.json'))
