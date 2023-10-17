import logging.config
import traceback
from pyspark.sql.functions import *
from pyspark.sql import *
from udf import split_and_count
logging.config.fileConfig('properties/configuration/logging.conf')
logger = logging.getLogger('data_transformation')


def data_report1(df_city,df_pres):
    try:
        logger.warning('Start data_report1 method =====')
        logger.warning('Count zip')
        df_city =df_city.withColumn('zip_count',split_and_count('zips'))
        logger.warning('Count prescriber id and sum total claim by city')
        df_pres_agg = df_pres.groupBy('city','state').agg(sum('total_claim_count').alias('total_claim_count'),count('presc_id').alias('total_presc'))
        df_final = df_pres_agg.join(df_city,(df_city.city == df_pres_agg.city) & (df_city.state_id ==df_pres_agg.state),'inner').drop(df_city.city)
        # df_final.show()
        df_final= df_final.select('city','state','county_name','population','density','zip_count','total_presc','total_claim_count')
        
    except Exception as e:
        logger.error(f'An error occurs in data_report1 {traceback.print_exc()}')
        raise
    else:
        logger.warning('Data report done...........')
    return df_final


def data_report2(df_pres):
    try:
        logger.warning('Start data_report1 method =====')
        logger.warning('Count zip')
        part = Window.partitionBy('state').orderBy(col('total_claim_count').desc())
        df_pres = df_pres.select('presc_id','full_name','drug_name','city','state','Country_name','total_claim_count','years_of_exp','total_day_supply') \
                .filter((col('years_of_exp')>20)&(col('years_of_exp')<50)).withColumn('dense_rank',dense_rank().over(part)) \
                .filter(col('dense_rank') <=5)
        # df_pres.show()
        
    except Exception as e:
        logger.error(f'An error occurs in data_report1 {traceback.print_exc()}')
        raise
    else:
        logger.warning('Data report done...........')
    return df_pres