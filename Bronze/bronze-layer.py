# Databricks notebook source
# mount the adlsgen2
isMount = False
for x in dbutils.fs.mounts():
    if x.mountPoint == '/mnt/source':
        isMount = True
        break;
    else:
        isMount = False

print(isMount)

if not isMount:
    dbutils.fs.mount(source='wasbs://source@adlsgendatalake.blob.core.windows.net',mount_point='/mnt/source',
    extra_configs={'fs.azure.account.key.adlsgendatalake.blob.core.windows.net':dbutils.secrets.get(scope='storage_key',key='storagekey')})

    print('Mountated sucessfully ')
    isMount = True


# COMMAND ----------

lending_raw_df = spark.read.format('csv').option('header',True).option('inferschema',True).load('/mnt/source/lending_raw_data.csv')

# COMMAND ----------

# read the status lookup file to load only relevent data into bronze layer
loan_status_lkp = spark.read.format('csv').option('header',True).option('inferschema',True).load('/mnt/source/loan_status.csv')
loan_status_lkp.createOrReplaceTempView('loan_status')

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

lending_raw_df.createOrReplaceTempView('lending_data')
invalid_records_df = spark.sql("select * from lending_data where loan_status not in (select * from loan_status) or loan_status is  null")
valid_records_df = spark.sql("select * from lending_data where loan_status is not null")
print(invalid_records_df.count())

# COMMAND ----------

# write the invalid records and valid records
invalid_records_df.write.mode('overwrite').option('header',True).format('csv').save('/mnt/source/invalid_data/')
valid_records_df.write.mode('overwrite').option('header',True).save('/mnt/source/Bronze/')

# COMMAND ----------


