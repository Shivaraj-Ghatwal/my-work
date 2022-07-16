#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("test").getOrCreate()


# # reading csv file

# In[2]:


df=spark.read.csv(r"C:\Users\Shivaraj\OneDrive\Desktop\my work\assignment1\result.csv",header=True,inferSchema=True)
df.show(60,truncate=False)


# # Pivoting the data

# In[3]:


df1=df.groupBy("NAME").pivot("SUBJECT").max("MARKS")
df1.show()


# # assign rank value using ntile(set vale 2 for pass 1 and fail 2)

# In[4]:


from pyspark.sql.window import Window
windowSpec=Window.partitionBy("NAME").orderBy("ENG")
from pyspark.sql.functions import ntile

df2= df1.withColumn("pass_fail",ntile(2).over(windowSpec)).     select("NAME","ENG","HINDI","KAN","MATHS","SCIENCE","SOCIAL",     (df1.ENG>=35) & (df1.HINDI>=35) & (df1.KAN>=35) & (df1.MATHS>=35) & (df1.SCIENCE>=35) & (df1.SOCIAL>=35)).     withColumnRenamed("((((((ENG >= 35) AND (HINDI >= 35)) AND (KAN >= 35)) AND (MATHS >= 35)) AND (SCIENCE >= 35)) AND (SOCIAL >= 35))","Result")

df2.show(truncate=False)


# # counting total 

# In[5]:


df3=df2.withColumn("Total",df2["ENG"]+df2["HINDI"]+df2["KAN"]+df2["MATHS"]+df2["SCIENCE"]+df2["SOCIAL"])
df3.show()


# In[6]:


from pyspark.sql.functions import *

df4=df3.withColumn("Final_Result",regexp_replace("Result","true","Pass")).na.replace("false",'Fail').drop("Result")
df4.show()


# # calculate percentage

# In[7]:


df5=df4.withColumn('Percent',(col('Total')/600)*100) 


# In[13]:


df5.show()


# # listing students result one below the other 

# In[9]:


df5.select("*",round("Percent",2)).drop('Percent').withColumnRenamed("round(Percent, 2)","Percent").show(truncate=False,vertical=True )


# In[10]:


df5.columns


# # Class Toper Name

# In[11]:


df5.summary("max").show()


# # Total student names 

# In[14]:


df5.createOrReplaceTempView('my_temp')
spark.sql("select count(*) from my_temp").show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




