import get_env_variables as gev
from create_spark import get_spark_object
from validate import get_current_date
from ingest import load_file,display_df,validate_df
import logging
import logging.config


logging.config.fileConfig('properties/configuration/logging.conf')


def main():
    try :
        logging.info('Calling spark object')
        spark =get_spark_object(gev.envn,gev.appName)
        logging.info('Validate spark object')
        get_current_date(spark)
        logging.info('Load file ')
        # df1= load_file(spark,folder='olap')
        df2 = load_file(spark,folder='oltp')
        logging.info('Show dataframe :')
        display_df(df2)
        logging.info('Validate dataframe')
        validate_df(df2)
    except Exception as e:
        logging.error(f'An error in main() : {str(e)}')



if __name__== '__main__':
    main()    
    logging.info('--------------Application done----------------')
