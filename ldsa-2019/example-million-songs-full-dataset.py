#!/usr/bin/env python
# coding: utf-8

# In[3]:


from pyspark.sql import SparkSession

# New API
spark_session = SparkSession        .builder        .master("spark://192.168.1.153:7077")         .appName("ben_million_songs")        .config("spark.dynamicAllocation.enabled", True)        .config("spark.shuffle.service.enabled", True)        .config("spark.dynamicAllocation.executorIdleTimeout","30s")        .config("spark.executor.cores",4)        .getOrCreate()

# Old API (RDD)
spark_context = spark_session.sparkContext


# In[ ]:


def split_line(line):
    return line.split(' ')


lines = spark_context.textFile("/mnt/ms/data/A/A/*")
print(lines.first())
lines_splitted = lines.flatMap(split_line)
print(lines_splitted.take(10))


# In[ ]:





# In[ ]:


# release the cores for another application!
# spark_context.stop()

