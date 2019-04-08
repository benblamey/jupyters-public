#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession

# (8 cores, 16gb per machine) x 5 = 40 cores

# New API
spark_session = SparkSession        .builder        .master("spark://localhost:7077")         .config("spark.driver.host","130.238.182.249")        .config("spark.driver.port",5000)        .config("spark.blockManager.port",5001)        .appName("blameyben_lecture1_simple_example")        .config("spark.dynamicAllocation.enabled", True)        .config("spark.shuffle.service.enabled", True)        .config("spark.dynamicAllocation.executorIdleTimeout","10s")        .config("spark.dynamicAllocation.initialExecutors",1)        .config("spark.dynamicAllocation.maxExecutors",4)        .getOrCreate()

# Old API (RDD)
spark_context = spark_session.sparkContext


# In[ ]:


def add(a, b):
    # commutative and associative!
    return a + b

rdd = spark_context.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)

result = rdd.map(lambda x: x * 2)            .reduce(add)

print(result)

#See: http://spark.apache.org/docs/2.3.0/api/python/pyspark.html


# In[ ]:


# release the cores for another application!
# spark_context.stop()


# In[ ]:




