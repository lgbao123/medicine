import logging.config
import get_env_variables as gev
import os
logging.config.fileConfig('properties/configuration/logging.conf')
logger = logging.getLogger('ingest')

def get_info_file(file ,src):
    file_dir = src + '/'+ file
    if file.endswith('.parquet'):
        format= 'parquet'
        header ='NA'
        inferSchema = 'NA'
    elif file.endswith('.csv'):
        format= 'csv'
        header =gev.header
        inferSchema = gev.inferSchema
    return file_dir,format,header,inferSchema

def load_file(spark ,folder):
    try :
        logger.warning('Start load file method ======')
        if folder  =='olap':
            src = gev.source_olap
        else:
            src =gev.source_oltp

        for file in os.listdir(src):
            file_dir,format,header,inferSchema = get_info_file(file,src)
            if format == 'parquet':
                df = spark.read.format(format).load(file_dir)
            elif format == 'csv':
                df =spark.read.format(format).options(header=header).options(inferSchema=inferSchema).load(file_dir)
    except Exception as e:
        logger.error(f'An error in occurs in load_file(): {str(e)}')

        raise
    else:
        logger.warning(f'Load file done which is of {format}..........')
    
    return df

def display_df(df):
    try:
        return df.show()
    except Exception as e:
        logger.error(f'An error in occurs in display_df(): {str(e)}')
        raise

def validate_df(df):
    logger.warning('Start validate df method =====')
    try:
        df_count = df.count()
    except Exception as e:
        logger.error(f'An enrror occurs validate_df(): {str(e)}')
    else:
        logger.warning(f'Number record df: {df_count}')
