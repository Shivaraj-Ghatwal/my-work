#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql import SparkSession,types
spark = SparkSession.builder.appName('practice').getOrCreate()


# In[2]:


df=spark.read.csv('C:/Users/Shivaraj/pyspark work/supermarket_sales1.csv',header=True)


# In[3]:


df.show()

# row_number Window Function

row_number() window function is used to give the sequential row number starting from 1 to the result of each window partition.
# In[4]:


# row_number

from pyspark.sql.window import Window
from pyspark.sql.functions import *
windowSpec=Window.partitionBy("City").orderBy("Total")

df.withColumn("row_number",row_number().over(windowSpec)).show(truncate=False)


# # rank Window Function
rank() window function is used to provide a rank to the result within a window partition.
This function leaves gaps in rank when there are ties.
# In[5]:


df.withColumn("rank",rank().over(windowSpec)).show()


# # dense_rank Window Function
# 
dense_rank() window function is used to get the result with rank of rows within a window partition without any gaps. 
This is similar to rank() function difference being rank function leaves gaps in rank when there are ties.
# In[6]:


"""dens_rank"""
from pyspark.sql.functions import dense_rank
df.withColumn("dense_rank",dense_rank().over(windowSpec)).show()


# # percent_rank Window Function

# In[7]:


""" percent_rank """
from pyspark.sql.functions import percent_rank
df.withColumn("percent_rank",percent_rank().over(windowSpec)).show()


# # ntile Window Function
ntile() window function returns the relative rank of result rows within a window partition.
In below example we have used 2 as an argument to ntile hence it returns ranking between 2 values (1 and 2)
# In[8]:


"""ntile"""
from pyspark.sql.functions import ntile
df.withColumn("ntile",ntile(2).over(windowSpec))     .show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




