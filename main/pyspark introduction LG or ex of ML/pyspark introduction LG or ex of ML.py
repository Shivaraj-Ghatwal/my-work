#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('missing').getOrCreate()


# In[5]:


training=spark.read.csv('test1.csv',header=True,inferSchema=True)


# In[6]:


training.show()


# In[7]:


training.printSchema()


# In[10]:


training.columns


# In[13]:


### [Age,Experience]----> new feature--->independent feature


# In[14]:


from pyspark.ml.feature import VectorAssembler

featureassembler=VectorAssembler(inputCols=["age","Experience"],outputCol="Independent Features")


# In[15]:


output=featureassembler.transform(training)


# In[16]:


output.show()


# In[17]:


output.columns


# In[18]:


finalized_data=output.select("Independent Features","Salary")


# In[19]:


finalized_data.show()


# In[20]:


from pyspark.ml.regression import LinearRegression

##train test split

train_data,test_data=finalized_data.randomSplit([0.75,0.25])
regressor=LinearRegression(featuresCol='Independent Features', labelCol='Salary')
regressor=regressor.fit(train_data)


# In[21]:


### Coefficients
regressor.coefficients


# In[22]:


### Intercepts
regressor.intercept


# In[23]:


### Prediction
pred_results=regressor.evaluate(test_data)


# In[24]:


pred_results.predictions.show()


# In[26]:


pred_results.meanAbsoluteError,pred_results.meanSquaredError


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




