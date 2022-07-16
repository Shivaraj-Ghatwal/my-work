#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession


# In[3]:


spark=SparkSession.builder.appName('agg').getOrCreate()


# In[4]:


df_pyspark=spark.read.csv('test3.csv',header=True,inferSchema=True)


# In[5]:


df_pyspark.show()


# In[6]:


## Group by who is having maXIMUN SALARY

df_pyspark.groupBy('name').sum().show()


# In[8]:


# which diparment is paying more salary

df_pyspark.groupBy('departments').sum().show()


# In[28]:


# MAXIMUM SALRY IN THE LIST

df_pyspark.groupBy('name').max().show()


# In[10]:


# find mean

df_pyspark.groupBy('departments').mean().show()


# In[11]:


# no of emploies are working in each departmets

df_pyspark.groupBy('departments').count().show()


# In[7]:


# find total sum of the salary

df_pyspark.agg({'salary':'sum'}).show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




