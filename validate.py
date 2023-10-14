import logging.config
logging.config.fileConfig('properties/configuration/logging.conf')
logger = logging.getLogger('validate')

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