# Databricks notebook source
# Read the bronze layer
raw_df = spark.read \
.format("delta") \
.option("InferSchema","true") \
.option("header","true") \
.load("/mnt/source/Bronze/")

# COMMAND ----------



# COMMAND ----------

# genrate the member id 
from pyspark.sql.functions import sha2,concat_ws
new_df = raw_df.withColumn("name_sha2", sha2(concat_ws("||", *["emp_title", "emp_length", "home_ownership", "annual_inc", "zip_code", "addr_state", "grade", "sub_grade","verification_status"]), 256))


# COMMAND ----------

new_df.createOrReplaceTempView("newtable")

# COMMAND ----------

customer_data_df = spark.sql("""select name_sha2 as member_id,emp_title,emp_length,home_ownership,annual_inc,addr_state,zip_code,'USA' as country,grade,sub_grade,
verification_status,tot_hi_cred_lim,application_type,annual_inc_joint,verification_status_joint from newtable
""")

# COMMAND ----------

customer_data_df.write.mode('overwrite').option('header',True).save('/mnt/source/Silver/customers_raw_data/')

# COMMAND ----------

spark.sql("""select id as loan_id, name_sha2 as member_id,loan_amnt,funded_amnt,term,int_rate,installment,issue_d,loan_status,purpose,
title from newtable""").write \
.option("header",True)\
.mode("overwrite") \
.option("path", "/mnt/source/Silver/loans_raw_data/") \
.save()

# COMMAND ----------

spark.sql("""select id as loan_id,total_rec_prncp,total_rec_int,total_rec_late_fee,total_pymnt,last_pymnt_amnt,last_pymnt_d,next_pymnt_d from newtable""").write \
.option("header",True)\
.mode("overwrite") \
.option("path", "/mnt/source/Silver/loans_repayments_raw_data/") \
.save()

# COMMAND ----------

spark.sql("""select name_sha2 as member_id,delinq_2yrs,delinq_amnt,pub_rec,pub_rec_bankruptcies,inq_last_6mths,total_rec_late_fee,mths_since_last_delinq,mths_since_last_record from newtable""").write \
.option("header",True)\
.mode("overwrite") \
.option("path", "/mnt/source/Silver/loans_defaulters_raw_data") \
.save()

# COMMAND ----------


