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

dbutils.fs.ls('/mnt/source')



# COMMAND ----------

lending_raw_df = spark.read.format('csv').option('header',True).option('inferschema',True).load('/mnt/source/lending_raw_data.csv')

# COMMAND ----------

status_df = lending_raw_df.filter(lending_raw_df['loan_status'].isNull()).count()
display(status_df)

# COMMAND ----------


