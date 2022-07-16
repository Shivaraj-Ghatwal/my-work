#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkContext 
spark= SparkSession.builder.appName("test").getOrCreate()


# In[2]:


df=spark.read.format("csv").load("C:/Users/Shivaraj/pyspark work/supermarket_sales1.csv",header=True,inferSchema=True)


# In[3]:


df.printSchema()


# In[4]:


df.show(10,False)


# In[5]:


df2= df.filter(df['Total'] >= 500)


# In[6]:


df2= df.filter(
    (df['Product line'] == 'Health and beauty') & 
    (df['Total']>=500))

df2.show()


# In[7]:


# df.groupBy('Health and beauty').min('Total').show()

df3= df.filter(
    (df['Product line'] == 'Health and beauty'))
df3.show()


# In[8]:


df3.count()


# In[9]:


(df3.groupBy('Total').avg()).show()


# In[10]:


from pyspark.sql.functions import *

df4=df.groupBy('Product line').avg('Total').select('Product line',col("avg(Total)").alias("average"))
df4.show()


# In[11]:


df4.sort(col('average').desc()).show()


# In[12]:


df2.show()


# In[13]:


df.rdd.getNumPartitions()


# In[14]:


type(df2)


# In[15]:


df.write.format("json").save('C:/Users/shivaraj/outputnew3')


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




