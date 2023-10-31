#!/usr/bin/env python
# coding: utf-8

# Credit Card Fraud Detection

# In[65]:
#Sigmoid function = Y^ =1/1+e^z

#updating weights through gradient descent 
#w2=w1-L*dw
#b2=b1-L* db

#Derivatives

#dw=1/m *(Y^)

#step 1: Learning rate and number of iteration ;Initiate random weight and biasvalue 
#step 2: Build 


import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score


# In[5]:


Credit=pd.read_csv("creditcard.csv")


# In[8]:


Credit.head()
#in this dataset we have time in seconds and currency in $ 


# In[9]:


Credit.info()


# In[10]:


#checking the missing value in each column  
Credit.isnull().sum()


# In[13]:


#Distribution for legit transaction and fraud transaction 

Credit['Class'].value_counts()
#--->0 means normal transaction (284315)
#--->1 means fraud transaction (492)

#Data is so un-balanced  we cannot feed this data to our machine learning model 


# In[15]:


#Seprating the data for analysis

Legit_transaction=Credit[Credit.Class==0]
Fraud_transaction=Credit[Credit.Class==1]


# In[21]:


print(Legit_transaction.shape)
print(Fraud_transaction.shape)


# In[22]:


#Stat Measures
Credit.Amount.describe()


# In[24]:


Fraud_transaction.Amount.describe()


# In[25]:


#Compare the transaction of both fraud and legit
Credit.groupby('Class').mean()


# In[32]:


#dealing with un-balanced data by taking sample data set making similaar distribution between bth the transaction 
legit_sample=Legit_transaction.sample(n=492)


# In[33]:


fraud_sample=Fraud_transaction.sample(n=492)

legit_sample.shape
# 

# In[35]:


legit_sample.shape


# In[36]:


fraud_sample.shape


# In[38]:


#creating new dataset
new_dataset=pd.concat([legit_sample,fraud_sample],axis=0)


# In[39]:


new_dataset


# In[40]:


new_dataset['Class'].value_counts()


# In[42]:


new_dataset.groupby('Class').mean()


# In[44]:


#Splitting the data into features and target

X=new_dataset.drop(columns='Class' ,axis=1)


# In[46]:


#labels 0,1
Y=new_dataset['Class']


# In[48]:


Y


# In[51]:


#spliting the data into training data and testing data
#stratify measns evenly distribution of data in training and testing 

X_train,X_test,Y_train,Y_test =train_test_split(X,Y ,test_size=0.2 ,stratify=Y,random_state=2)


# In[52]:


X_train.shape

X_test.shape
# In[53]:


X_test.shape


# In[68]:


model=LogisticRegression()
               
                 
        


# 

# In[70]:


model.fit(X_train,Y_train)


# In[74]:


#accuracy_score
X_train_prediction=model.predict(X_train)
training_data_accuracy=accuracy_score(X_train_prediction ,Y_train)


# In[76]:


print(training_data_accuracy*100)


# In[77]:


X_test_prediction=model.predict(X_test)
test_data_accuracy=accuracy_score(X_test_prediction ,Y_test)


# In[78]:


print(test_data_accuracy*100)


# In[ ]:




