#!/usr/bin/env python
# coding: utf-8

# In[88]:


from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("test").getOrCreate()
df=spark.read.csv(r"C:\Users\Shivaraj\supermarket_sales1.csv")
df.show(10,False)


# In[89]:


cols=df.columns
cols


# # modifing  column names 

# In[58]:


from pyspark.sql.functions import *
from functools import *
df1=reduce(lambda new_df, i : new_df.withColumnRenamed(i, i.replace(" ","_")),cols,df)


# In[59]:


df1.show()


# # Completely changing colums names

# In[90]:


from pyspark.sql.functions import *
from functools import *
colms=df.columns

new_cols=["Invoice","City_Name","Genders","Products","Unit_price","Quantity","5%_Tax","Total_amount","Date"]

new_df1=reduce(lambda df2, i: df2.withColumnRenamed(colms[i], new_cols[i]), range(len(cols)), df)


# In[91]:


new_df1.show()


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




