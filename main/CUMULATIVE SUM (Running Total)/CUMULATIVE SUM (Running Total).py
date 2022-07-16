#!/usr/bin/env python
# coding: utf-8

# # CUMULATIVE SUM (Running Total)OF COLUMN AND GROUP IN PYSPARK 

# In[2]:


from pyspark.sql.window import Window
from pyspark.sql import SparkSession,types
spark = SparkSession.builder.appName('practice').getOrCreate()


# In[4]:


df=spark.read.csv('C:/Users/Shivaraj/pyspark work/supermarket_sales1.csv',header=True)
df.show()


# In[6]:


import sys
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import *
cum_sum = df.withColumn('cumsum', f.sum(df.Total).over(Window.partitionBy('Product line').orderBy('Invoice ID').rowsBetween(-sys.maxsize, 0)))
cum_sum.select("*",round("cumsum",2)).drop('cumsum').withColumnRenamed("round(cumsum, 2)","cumsum").show(8,truncate=False)


# In[7]:


import sys

from pyspark.sql.window import Window
import pyspark.sql.functions as F
cumulative=df.withColumn("CumlativeSum",f.sum(df.Total).over(Window.partitionBy().orderBy().rowsBetween(-sys.maxsize, 0)))
cumulative.select("*",round("CumlativeSum",2)).drop('CumlativeSum').withColumnRenamed("round(CumlativeSum, 2)","CumlativeSum").show(truncate=False)


# In[8]:


df.show()


# In[9]:


df.select("*",round("Total",2)).drop("Total").withColumnRenamed("round(Total, 2)","Total").show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




