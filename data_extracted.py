import pandas as pd
import ast
import json
import numpy as np

data=pd.read_csv('new_final_data.csv')

data['tr_decoded']=data['tr_decoded'].map(lambda x:x.lstrip("b'").rstrip("'").replace('\\"','in.'))
data.timestamp=pd.to_datetime(data.timestamp)
data.drop('event',axis=1,inplace=True)

def parse(string):
    try: 
        # print(string)
        return ast.literal_eval(str(string))[0]
    except:
        return 'Problem data'

def feed_parse(x):
    feed_data={}
    try:
        for i in x:   
            feed_data[i['key']]=i['value']
        return feed_data
    except:
        return ''


# Parsing data tr_decoded column
data['tr_data']=data['tr_decoded'].map(lambda x: parse(x) if x!='' else '')
parsed_df=data['tr_data'].apply(pd.Series)
print(type(parsed_df),parsed_df.columns)

# Dropping unwanted columns from parsed_df
parsed_df.drop([0],axis=1,inplace=True)

# concatenating the two dataframes
data=pd.concat([data, parsed_df], axis=1)
data.drop(['Unnamed: 0'],axis=1,inplace=True)

data['feed']=data['feed'].map(feed_parse)


data.to_csv('parsed_data_v2.csv')
# event_key_items=['view_item','view_cart','begin_checkout','add_to_wishlist','add_to_cart','purchase','remove_from_cart']

print('Done')
