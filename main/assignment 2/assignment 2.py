#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[2]:



from pyspark.sql import SparkSession,types

spark = SparkSession.builder.master('local').appName('qtn1').getOrCreate()

sc=spark.sparkContext


# In[3]:


df=spark.read.text('C:/Users/Shivaraj/pyspark work/book1.csv')


# In[4]:


df.show(truncate=0)


# In[5]:


header=df.first()[0]
header


# In[ ]:





# In[6]:


schema=['fname','lname','age','dep']
print(schema)


# # filter the header, separate the columns and apply the schema

# In[7]:




df_new=df.filter(df['value']!=header).rdd.map(lambda x:x[0].split('|')).toDF(schema)
df_new.show()


# # concat the columns “fname” and “lname” :

# In[8]:



from pyspark.sql.functions import *
df1=df_new.withColumn('Name',concat(col('fname'),lit(' '),col('lname')))
df1.show()


# # attaching 2 columns one below the other

# In[9]:


a=df1.select('fname')

b=df1.select('lname')

c=a.union(b).show()


# In[10]:


df1.drop('fname','lname').select('name','age','dep').show()


# # marge two columns

# In[11]:



df2=df_new.select(concat(df_new.fname,lit(" "),df_new.lname).alias("Fullnames"),'age','dep')


# In[12]:


df2.show()


# # split column 

# In[13]:


df3=df2.withColumn("Fristname",split(col("Fullnames")," ").getItem(0)).withColumn("lastname",split(col("Fullnames")," ").getItem(1)).drop("Fullnames")
df3.show()


# In[14]:


df5=df3.select(concat(df3.Fristname,lit(" "),df3.lastname).alias("fullname"),"age","dep")
df5.show()


# # selcet multiple columns

# In[15]:


df5.select(['age','dep']).show()


# # Renaming colums

# In[16]:


df6=df5.withColumnRenamed('dep','Stream')
df6.show()


# # coalesce key
# The COALESCE() function returns the first non-null value in a list.
# In[17]:


qf=spark.read.csv("D:/Shivaraj/learning/dataset-master/dataset-master/mob_num.csv",header=True,inferSchema=True)
qf.show()


# In[18]:


# collect all people mobile number if available, if not mention not available

output = qf.withColumn("new_Number",coalesce("Personal_Mobile","Home_Mobile","office_no",lit("not_available")))
output.show()


# In[19]:


output.select("Name","new_Number").show()


# In[21]:


output.rdd.getNumPartitions()


# In[27]:


output.select("Name").distinct().show()


# In[28]:


output.count()


# In[41]:


output.rdd.repartition(3).getNumPartitions()


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




