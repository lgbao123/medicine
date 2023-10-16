import logging.config
logging.config.fileConfig('properties/configuration/logging.conf')
logger = logging.getLogger('validate')
from pyspark.sql.functions import *
def get_current_date(spark):
    try:
        logger.warning('Start get_current_date method=====')
        output = spark.sql("Select current_date()").collect()[0][0]
        logger.warning('Validate spark object with current date :' + str(output))
    except Exception as e:
        logger.error(f'An error occurs in get_current_date() : {str(e)}')
        raise
    else:
        logger.warning('Validation done ...')

def print_data_schema(df , df_name):
    try:
        logger.warning(f'Start print_data_schema method ====({df_name})')
        fields = df.schema.fields
        for i in fields:
            logger.warning(f'\t\t{i}')
    except Exception as e:
        logger.error(f'An error occurs in print_data_schema() : {str(e)}')
        raise
    else:
        logger.warning('print_data_schema done ...')

def check_null_df(df,df_name):
    try:
        logger.warning(f'Start check_null_df method ====({df_name})')
        df = df.select([count(when(isnull(c)| isnan(c),1)).alias(c) for c in df.columns])
    except Exception as e:
        logger.error(f'An error occurs in check_null_df() : {str(e)}')
        raise
    else:
        logger.warning('check_null_df done ...')
    return df