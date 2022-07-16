#!/usr/bin/env python
# coding: utf-8

# # filter operations
#     and &
#     or |
#     not ~

# In[16]:


from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('dataframe').getOrCreate()

df_pyspark=spark.read.csv('test1.csv',header=True,inferSchema=True)


# In[18]:


# salary lesser than or equel to 20000

df_pyspark.filter("Salary<=20000").show()


# In[22]:


# select only two column name and age

df_pyspark.filter("Salary<=20000").select(['name','age']).show()


# In[23]:


df_pyspark.filter((df_pyspark['salary'])<=20000).show()


# In[1]:


# & AND OPERATOR

df_pyspark.filter(
    (df_pyspark['salary']<=20000) &
    (df_pyspark['salary']>=16000)).show()


# In[34]:


# & AND OPERATOR

df_pyspark.filter(
    (df_pyspark['salary']<=20000) &
    (df_pyspark['age']<=30)).show()


# In[36]:


# | OR OPERATOR

df_pyspark.filter(
    (df_pyspark['salary']<=20000) |
    (df_pyspark['age']<=30)).show()


# In[38]:


# ~ NOT OPERATOR

df_pyspark.filter(
            (df_pyspark['salary']>=30000) |
            (~(df_pyspark['age']<=29))
                 ).show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




