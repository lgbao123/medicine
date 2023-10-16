from pyspark.sql.functions import *
from pyspark.sql.types import *

@udf(returnType=IntegerType())
def split_and_count(column):
    return len(column.split(' '))