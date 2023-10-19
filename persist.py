
import logging.config
logging.config.fileConfig('properties/configuration/logging.conf')
logger = logging.getLogger('persist')

def save_data_to_hive(spark,df,dbname,dfname,partition,mode):
    try:
        logger.warning(f'Start save_data_to_hive {dfname}======')
        spark.sql(f'create database if not exists {dbname}')
        spark.sql(f'use {dbname}')
        df.write.saveAsTable(dfname, partitionBy =partition , mode = mode)
    except Exception as e:
        logger.error(f'An error occurs in save_data_to_hive {e}')
        raise
    else:
        logger.warning('save_data_to_hive() done.............')