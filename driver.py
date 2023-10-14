import get_env_variables as gev
from create_spark import get_spark_object
from validate import get_current_date,print_data_schema
from ingest import load_file,display_df,validate_df
from data_processing import clean_data
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
        # logging.info('Show dataframe :')
        # display_df(df2)
        logging.info('Validate dataframe')
        validate_df(df2)
        logging.info('Processing dataframe')
        df2 = clean_data(df2)
        logging.info('Show dataframe :')
        display_df(df2)
        print_data_schema(df2,'Presc_df')

    except Exception as e:
        logging.error(f'An error in main() : {str(e)}')



if __name__== '__main__':
    main()    
    logging.info('--------------Application done----------------')
