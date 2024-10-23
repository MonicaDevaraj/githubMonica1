import os
import urllib.request

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt": os.path.join(data_dir, "test.txt"),
}

for url, path in urls_and_paths.items():
    urllib.request.urlretrieve(url, path)


# ======================================================================================

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import sys
import os
from collections import namedtuple
from pyspark.sql.functions import *
from pyspark.sql.types import *

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'C:\Users\44772\.jdks\corretto-1.8.0_422'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

#spark.read.format("csv").load("data/test.txt").toDF("Success").show(20,False)


#####################################################################################

#########################################################################################

rawstr = ['Monica', 'Monu', 'Lucky']
print()
print("\033[92m===========RAW STR=============\033[0m")
print()
print(rawstr)####prints the raw string

rdd= sc.parallelize(rawstr)#########coverts raw string to rdd
print()
print("\033[91m================RDD LIST===========\033[0m")
print(rdd.collect())

addstr = rdd.map ( lambda x : x + "Devaraj")### appends the given data
print()
print("\033[93m===========ADDED STR===============\033[0m")
print(addstr.collect())

replacelist = addstr.map(lambda x : x .replace("Mon", "Zon"))##### replaces the given data
print()
print("\033[94m========Replaced data===========\033[0m")
print(replacelist.collect())

filterlist = addstr.filter(lambda x : 'Monu' in x )
print()
print("\033[95m======Filter list=====\033[0m")##### Filtered the data
print(filterlist.collect())

# Create an RDD with data like "A~B", "C~D"
data = ["A~B", "C~D"]
print("\033[91m================RAW DATA===========\033[0m")
print(data)
# Parallelize the data into an RDD
rdd = sc.parallelize(data)
print("\033[95m================RDD DATA===========\033[0m")
print(rdd.collect())

# Use flatMap to split each string by the tilde (~)
flat_rdd = rdd.flatMap(lambda x: x.split("~"))

# Collect the result
print("\033[99m================FLATTEND DATA===========\033[0m")

result = flat_rdd.collect()
print(result)
########
mylist=["State->TN~City->Chennai","State->Kerala~City->Trivandrum"]
print("=============Raw List============")
print(mylist)

rdd=sc.parallelize(mylist)
print("=============RDD List============")
print(rdd.collect())

rddsplit = rdd.flatMap(lambda x : x.split("~"))
print("=============Split Data ============")
print(rddsplit.collect())

statefilter = rddsplit.filter(lambda x : 'State' in x)
print("=============State Data ============")
print(statefilter.collect())

cityfilter = rddsplit.filter(lambda x : 'City' in x)
print("=============City Data ============")
print(cityfilter.collect())

replacestate=statefilter.map(lambda x:x.replace("State->",""))
print("=============State updated Data ============")
print(replacestate.collect())

replacecity =cityfilter.map(lambda x:x.replace("City->",""))
print("=============City updated Data ============")
print(replacecity.collect())

