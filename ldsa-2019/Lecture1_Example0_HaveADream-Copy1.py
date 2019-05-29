#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession

# (8 cores, 16gb per machine) x 5 = 40 cores

# New API
spark_session = SparkSession        .builder        .master("spark://192.168.1.19:7077")         .appName("dream_king")        .config("spark.dynamicAllocation.enabled", True)        .config("spark.shuffle.service.enabled", True)        .config("spark.dynamicAllocation.executorIdleTimeout","30s")        .config("spark.executor.cores",4)        .getOrCreate()

# Old API (RDD)
spark_context = spark_session.sparkContext


# In[2]:


def split_line(line):
    return line.split(' ')


lines = spark_context.textFile("hdfs://192.168.1.19:9000/king-dream.txt")
print(lines.first())
lines_splitted = lines.flatMap(split_line)
print(lines_splitted.take(10))


# In[3]:


lines.flatMap(lambda line: line.split(' ')).filter(lambda word: word.startswith('d')).take(20)


# In[ ]:





# In[ ]:


# release the cores for another application!
# spark_context.stop()

