#!/usr/bin/env python
# coding: utf-8

# In[18]:


get_ipython().system('pip install pyspark')


# In[20]:


import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import *



# In[22]:


#start sparksession
spark=SparkSession.builder.getOrCreate()


# In[29]:


df=spark.read.option("header",True).option("infraSchema",True).csv("ipl_2022_dataset.csv")


# In[30]:


df.select("*").show()


# In[38]:


#total number of rows and column
print('Rows :', df.count())
print('Col : ' ,len(df.columns))


# In[39]:


#datattpes
df.printSchema()


# In[44]:


df.select('Base Price').distinct().show()


# In[47]:


#Dsitribution of base price in base price columns 

df.groupBy('Base Price').count().show()


# In[51]:


#Unique value in TYPE column
df.select('TYPE').distinct().show()


# In[62]:


#Renaming column 
df2=df.withColumnRenamed("TYPE","type").withColumnRenamed("COST IN ₹ (CR.)","cost_inr")


# In[63]:


#Droping the column
df3=df2.drop("Cost IN $ (000)")


# 

# In[64]:


df3.show()


# In[83]:


#Name top # batsman who get paid the most
df3.select('Player' ,'cost_inr').filter(df3.type == 'BATTER').orderBy(df3.cost_inr ,ascending=False).show(3)

spark.sql("select player , cost_inr from ipl_2022_dataset where type= 'BATTER' order by cost_inr desc limit 3").show()

# In[92]:


#Name top 5 bowler who get paid most
df3.select('Player','cost_inr').filter(df3.type=='BOWLER').orderBy('cost_inr',ascending=False).show(3)


# In[100]:


# Name 5 lowest paid wicket keeper
df3.select('Player','cost_inr').filter(df3.type=="WICKETKEEPER").orderBy('cost_inr',ascending=True).show(5)


# In[101]:


#What is the average pay for batsman ,bowler,keeper and allrounder?


# In[112]:


from pyspark.sql import functions as F
df3.groupBy('type').agg(F.round(F.mean(('cost_inr'),2).alias('average_price').show()


# In[113]:





# In[114]:


df.select('Base Price').distinct().show()


# ### 

# In[115]:


df4=df3.withColumnRenamed('Base Price','base_price')


# In[118]:


#List of retained player name with team name and salary
df4.select('Player','cost_inr','Team').where(df4.base_price=='Retained').orderBy('cost_inr',ascending=False).show()


# In[ ]:




