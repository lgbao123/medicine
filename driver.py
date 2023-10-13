import get_env_variables as gev
from create_spark import get_spark_object
from validate import get_current_date
import logging
import logging.config


logging.config.fileConfig('properties/configuration/logging.conf')


def main():
    try :
        logging.info('Calling spark object')
        spark =get_spark_object(gev.envn,gev.appName)
        logging.info('Validate spark object')
        get_current_date(spark)
    except Exception as e:
        logging.error(f'An error in main() : {str(e)}')



if __name__== '__main__':
    main()    
    logging.info('--------------Application done----------------')
