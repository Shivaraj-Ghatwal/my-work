#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[2]:




from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("dataset").getOrCreate()

from datetime import date 


# In[3]:


data=spark.createDataFrame([
    (1,"Shivaraj","Ghatwal",25,"BE",date(2015,6,1)),
    (2,"Ratesh","Ghatwal",28,"BA",date(2016,6,16)),
    (3,"Ajit","Ghatwal",26,"MBA",date(2013,3,7)),
    (4,"Ankita","Navage",29,"BBA",date(2012,7,11)),
    (5,"Prhlad","Mehtha",27,"BE",date(2011,8,10))
], schema = "sl_no long, First_name string, last_name string, age int, Education string, YOP date")


# In[5]:


data.show()


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





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




