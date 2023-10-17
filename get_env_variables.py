import os 

os.environ['envn'] = 'DEV'
os.environ['appName'] = 'SparkProject'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

envn = os.environ['envn']
appName = os.environ['appName']

header = os.environ['header']
inferSchema = os.environ['inferSchema']
current = os.getcwd()

source_oltp = current + '/src/oltp'
source_olap = current + '/src/olap'

city_path = 'output/city'
pres_path = 'output/pres'