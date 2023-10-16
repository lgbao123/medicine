import get_env_variables as gev
from create_spark import get_spark_object
from validate import get_current_date,print_data_schema,check_null_df
from ingest import load_file,display_df,validate_df
from data_processing import clean_data
from data_transformation import data_report1
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
        df_city= load_file(spark,folder='olap')
        df_pres = load_file(spark,folder='oltp')
        logging.info('Validate dataframe')
        validate_df(df_city)
        validate_df(df_pres)
        logging.info('Processing dataframe')
        df_pres,df_city= clean_data(df_pres,df_city)
        logging.info('Check null after processing')
        # check_null_df(df_city,'df_city').show()
        # check_null_df(df_pres,'df_pres').show()
        logging.info('Data transformation')
        df_report1 =data_report1(df_city,df_pres)
        df_report1.show()
        # logging.info('Show dataframe :')
        # display_df(df2)
        # print_data_schema(df2,'Presc_df')

    except Exception as e:
        logging.error(f'An error in main() : {str(e)}')



if __name__== '__main__':
    main()    
    logging.info('--------------Application done----------------')
