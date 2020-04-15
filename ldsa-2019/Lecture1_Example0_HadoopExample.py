#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession

# (8 cores, 16gb per machine) x 5 = 40 cores

# New API
spark_session = SparkSession        .builder        .master("spark://192.168.1.19:7077")         .appName("hadoop_example")        .config("spark.dynamicAllocation.enabled", True)        .config("spark.shuffle.service.enabled", True)        .config("spark.dynamicAllocation.executorIdleTimeout","30s")        .config("spark.executor.cores",4)        .getOrCreate()

# Old API (RDD)
spark_context = spark_session.sparkContext


# In[2]:


lines = spark_context.textFile("hdfs://192.168.1.19:9000/foo.txt")
print(lines.first())

lines_splitted = lines.map(lambda line: line.split(' '))
print(lines_splitted.first())


# In[3]:


# release the cores for another application!
# spark_context.stop()


# In[ ]:




