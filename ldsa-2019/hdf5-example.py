#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession

# (8 cores, 16gb per machine) x 5 = 40 cores

# New API
spark_session = SparkSession        .builder        .master("spark://host-192-168-1-153-ldsa:7077")         .appName("benblamey_hdf5")        .config("spark.dynamicAllocation.enabled", True)        .config("spark.shuffle.service.enabled", True)        .config("spark.dynamicAllocation.executorIdleTimeout","60s")        .config("spark.executor.cores",1)        .getOrCreate()

# Old API (RDD)
spark_context = spark_session.sparkContext


# In[2]:


import h5py
import sys
import io

# h5py doesn't know anything about HDFS, and will be unable to work with the 'magic' HDFS filenames.
# As a workaround, we can load the file into memory with Spark and then read the file.


# Tip: download the HDF5 viewer from https://www.hdfgroup.org/downloads/hdfview/
# ...and open one of the files on your laptop to explore the structure.

# In[3]:


rdd = spark_context.binaryFiles("hdfs://host-192-168-1-153-ldsa:9000/millionsongs/data/A/A/A/TRAAAAW128F429D538.h5")
# or load all the files with some wildcards...
#rdd = spark_context.binaryFiles("hdfs://host-192-168-1-153-ldsa:9000/millionsongs/data/*/*/*/*")

def f(x):
    # return print(sys.path)
    
    # x[0] = filename
    # x[1] = binary content
    with h5py.File(io.BytesIO(x[1])) as f:
        # drill down with a path
        # we need to convert to a "python list" -- custom types won't survive the round-trip via Java...
        return list(f.keys())
        
        g = f['metadata']['artist_terms']
        # g is a 'dataset' type.
        return g[1]

rdd = rdd.map(f)
rdd.collect()


# In[ ]:





# In[ ]:




