#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession


# In[2]:


spark = SparkSession.builder.appName('Dataframe').getOrCreate()


# In[3]:


spark


# In[14]:


## read thye dataset
df_pyspark= spark.read.option('header','true').csv('test1.csv',inferSchema=True)


# In[17]:


df_pyspark.printSchema()


# In[18]:


df_pyspark=spark.read.csv('test1.csv',header=True,inferSchema=True)
df_pyspark.show()


# In[19]:


df_pyspark.printSchema()


# In[20]:


type(df_pyspark)


# In[22]:


df_pyspark.columns


# In[23]:


df_pyspark.head(3)


# In[24]:


df_pyspark.show()


# In[32]:


df_pyspark.select('name').show()


# In[34]:


df_pyspark.dtypes


# In[36]:


df_pyspark.describe().show()


# In[41]:


df_pyspark=df_pyspark.withColumn('experience after 2 years',df_pyspark['experience']+2)


# In[42]:


df_pyspark.show()


# In[43]:


df_pyspark.drop('experience after 2 years').show()


# In[45]:


df_pyspark.withColumnRenamed('name','new name').show()


# In[ ]:





# In[ ]:





# In[ ]:




