import snowflake.connector

sfOptions = {
    "sfURL": "enunusy-ad43289.snowflakecomputing.com",
    "sfUser": "MOHAKK23",
    "sfPassword": "7587061048@Mg",
    "sfDatabase": "MY_DB",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN"
}

def create_connection(schema_name):
    return snowflake.connector.connect(
        user=sfOptions["sfUser"],
        password=sfOptions["sfPassword"],
        account=sfOptions["sfURL"].split('.')[0],
        database=sfOptions["sfDatabase"],
        schema=schema_name,
        warehouse=sfOptions["sfWarehouse"]
    )