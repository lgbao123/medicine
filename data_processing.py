import logging.config
from pyspark.sql.functions import *

logging.config.fileConfig('properties/configuration/logging.conf')
logger = logging.getLogger('data_processing')


def clean_data(df,df_city):
    try:
        logger.warning('Start clean data method ====')
        logger.warning('Selecting a few columns and rename')
        df = df.select(
            df.npi.alias('presc_id'),df.nppes_provider_last_org_name.alias('last_name'),
            df.nppes_provider_first_name.alias('first_name'),df.nppes_provider_city.alias('city'),
            df.nppes_provider_state.alias('state'),df.specialty_description.alias('desc'),
            df.drug_name,df.total_claim_count,df.total_day_supply,df.years_of_exp
        )

        df_city =df_city.select(upper('city').alias('city') , 'state_id','county_fips','county_name','population','density','timezone','zips')
        logger.warning('Create a new column')
        df = df.withColumn('Country_name',lit('USA'))

        logger.warning('Convert "years_of_exp" from string to int')
        df = df.withColumn('years_of_exp',regexp_replace(col('years_of_exp') ,r'^=',''))
        df = df.withColumn('years_of_exp',col('years_of_exp').cast('int'))

        logger.warning('Create full name column with concating columns and drop column')
        df = df.withColumn('full_name',concat_ws(' ',col('first_name'),col('last_name')))
        col_drop=['first_name','last_name']
        df =df.drop(*col_drop)

        # df =df.select([column for column in df.columns if column not in col_drop]) # another way



        logger.warning('Drop null and replace value')
        df =df.dropna(subset=['presc_id','drug_name','total_claim_count'])
        avg_total = df.agg(avg('total_claim_count')).collect()[0][0]
        df=df.fillna(avg_total,'total_claim_count')



    except Exception as e:
        logger.error(f'An error occurs in clean_data(): {str(e)}')
        raise
    else:
        logger.warning('Clean data done.......')    
    return df,df_city