
# coding: utf-8

# In[1]:


from gcloud import storage


# In[2]:


client = storage.Client()


# In[4]:


bucket = client.get_bucket('pysenti-data')


# In[20]:


blobs = bucket.list_blobs(prefix='lexicon')
for b in blobs:
    if b.name.endswith('_5/'):
        category = b.name.split('/')[1]
        print(category)
        folder = bucket.list_blobs(prefix=b.name)
        for i in folder:
            if i.name.endswith('.csv'):
                bucket.copy_blob(blob=i,destination_bucket=bucket,new_name='lexicon-saved/'+category+'.csv')

