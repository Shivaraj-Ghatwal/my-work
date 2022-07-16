#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('dataframe').getOrCreate()


# In[2]:


df_pyspark=spark.read.csv('C:/Users/Shivaraj/pyspark work/test2.csv',header=True,inferSchema=True)


# In[3]:


df_pyspark.show()


# In[5]:


df_pyspark.describe()


# In[9]:


df_pyspark.dtypes


# In[6]:


df_pyspark.drop('name').show()


# In[8]:


df_pyspark.na.fill('no value',['age','Experience','Salary']).show()


# In[9]:


df_pyspark.show()


# In[10]:


df_pyspark.na.drop().show()


# In[12]:


#any=how all

df_pyspark.na.drop(how='all').show()


# In[13]:


#threshold

df_pyspark.na.drop(how='any',thresh=2).show()


# In[14]:


#subset

df_pyspark.na.drop(how='any',subset=['name']).show()


# In[15]:



df_pyspark.show()


# In[16]:


df_pyspark.na.fill('no valle').show()


# In[21]:


from pyspark.ml.feature import Imputer

imputer = Imputer(
    inputCols=['age', 'Experience', 'Salary'], 
    outputCols=["{}_imputed".format(c) for c in ['age', 'Experience', 'Salary']]
    ).setStrategy("median")


# In[22]:


# Add imputation cols to df

imputer.fit(df_pyspark).transform(df_pyspark).show()


# In[ ]:





# In[ ]:





# In[ ]:




