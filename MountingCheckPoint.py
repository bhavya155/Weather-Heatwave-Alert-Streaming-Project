# Databricks notebook source
client_id =dbutils.secrets.get('sp','client-id')
tenant_id = "90c0fa28-5905-43d7-9d89-06dafbb378d0"
service_credential =dbutils.secrets.get('sp','secret-key')
storage_account="devadlsprojects"
container="checkpoint"

# COMMAND ----------


# create the configs dictionary
configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": service_credential,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
          }

    
dbutils.fs.mount(
        source=f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
        mount_point=f"/mnt/{storage_account}/{container}",
        extra_configs=configs)

# COMMAND ----------


