from pyspark.sql import SparkSession

import logging.config
logging.config.fileConfig('properties/configuration/logging.conf')
logger = logging.getLogger('create_spark')

def get_spark_object(env , appName):
    try:
        logger.info('start get_spark_object method')
        if env == 'DEV':
            master = 'local[*]'
        else:
            master ='Yarn'
        logger.info(f'master is {master}')
        spark = SparkSession.builder.master(master).appName(appName).getOrCreate()
        
    except Exception as e:
        logger.error(f'An error occurs in get_spark_object() : {str(e)}')
        raise
    else :
        logger.info(f'Spark obect created :{spark}...')

    return spark