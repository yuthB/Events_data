import pandas as pd
import ast

data=pd.read_csv('parsed_data_v2.csv')

def parse(string):
    # try: 
    # print(string)
    return ast.literal_eval(str(string))
    # except:
    #     return 'Problem data'

dummies=pd.get_dummies(data['event'])
data=pd.concat([data,dummies],axis=1)
pd.set_option('display.max_rows',100)

data.loc[data['feed'].isna(),'feed']=''
data['feed_data']=data['feed'].map(lambda x: parse(x) if x!='' else '' )
parsed_df=data['feed_data'].apply(pd.Series)

print(parsed_df)

# concatenating the two dataframes
data=pd.concat([data, parsed_df], axis=1)
data.drop([0,'Unnamed: 0'],axis=1,inplace=True)
data.fillna('',inplace=True)
data['event']=data['event'].astype('str')+'_'+data['screen_name'].astype('str')
data['event']=data['event'].map(lambda x:x.lstrip('_').rstrip('_'))

data['count']=1
grouped_data=data.groupby('useridentifier').sum()
top_5_users=grouped_data.sort_values('count',ascending=False).head(5).index
print(top_5_users)
top_5_users_data=data[data['useridentifier'].isin(top_5_users)].loc[: ,['useridentifier','headers.x-session-id','id','event','timestamp','feed']]

special_user=data[data['useridentifier']=='4d96a0ca-262d-4f0f-b699-30817466a9f0'].loc[: ,['useridentifier','headers.x-session-id','id','event','timestamp','feed']]
top_5_users_data.to_csv('top_5_users_data.csv')
special_user.to_csv('special_user_data.csv')
print('Done')