import config.loggers
import logging
from config.sparksession import spark
from config.snowflake import sf_options
import requests

def data_cleaning(dataframe):
    dataframe_without_duplicates =  dataframe.dropDuplicates()
    cleaned_dataframe = dataframe_without_duplicates.fillna("Value not available.").select("userId", "id", "title", "body")
    return cleaned_dataframe
    
def ingestion(api, table_name):
    res = None
    try:
        res = requests.get(url = api)
        status = res.status_code
        if res != None and status == 200:
            response = res.json()
            dataframe = spark.createDataFrame(response)
            cleaned_dataframe = data_cleaning(dataframe)
            cleaned_dataframe.write.format("snowflake").options(**sf_options).option("dbtable", f"RAW_SCHEMA.{table_name}").mode("overwrite").save()
            logging.info("Raw data successfully stored in the database.")

    except Exception as e:
        logging.error(f"Error: {e} occured.")
        raise
