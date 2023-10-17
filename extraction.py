import logging.config
logging.config.fileConfig('properties/configuration/logging.conf')
logger = logging.getLogger('extraction')

def extract(df,format,path,num_part,header,compresstionType):
    try:
        logger.warning('start extract method========')
        df.coalesce(num_part).write.mode('overwrite').format(format).save(path,header=header,compresstion=compresstionType)
    except Exception as e:
        logger.error(f'An error occurs in extract {str(e)}')
        raise
    else:
        logger.warning('Extract done..............')