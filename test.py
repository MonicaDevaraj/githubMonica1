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
#1. List Iteration
#Scenario: You have a dataset of employee records, and each employee has a list of skills. You need to list all unique skills that employees possess.

# Step 1: Create an RDD for the employee dataset
employees = [
    ("John", ["Python", "Java", "SQL"]),
    ("Jane", ["Python", "Scala", "SQL"]),
    ("Doe", ["C++", "Java", "Python"]),
]

# Step 2: Parallelize the employee dataset to create an RDD
employee_rdd = sc.parallelize(employees)

# Step 3: Extract all the skills
# FlatMap is used to flatten the list of skills from each employee
skills_rdd = employee_rdd.flatMap(lambda employee: employee[1])

# Step 4: Remove duplicates by applying distinct
unique_skills_rdd = skills_rdd.distinct()

# Step 5: Collect the results
unique_skills = unique_skills_rdd.collect()

# Output the unique skills
print("Unique Skills:", unique_skills)

# Step 6: Stop the SparkContext
sc.stop()

#2 String Iteration
#Scenario: You have a dataset of sentences, and you want to analyze each word's frequency in the dataset. However, you need to remove any punctuation and treat all words in lowercase.

#Write a PySpark program that counts the frequency of each word after cleaning up the punctuation and converting the text to lowercase.


