import pandas as pd
import base64
import re

# Importing csv
events = pd.read_csv('40ba07c8-82e7-42e6-9703-02ecc7099667.csv')

# Replacing 'NaN' values with padding for base64 decoder to work
events['parameters.tr'].fillna('====', inplace=True)

# Cleaning non-ascii characters
events['parameters.tr'] = events['parameters.tr'].map(lambda x: x.strip().replace('-', '+'))


# function to convert base64 string to decodable string in utf-8
def str_decode(string):    
    if len(string) % 4 == 1:
        return 'Not Found'
    elif len(string) % 4 == 2 or len(string) % 4 == 3:      
        string = string + '=' * (4 - len(string) % 4)    
    else:
        string = string
    return base64.b64decode(string)

# function to search a pattern in a string
def search(pattern,string):
    mystr=re.compile(pattern)
    if mystr:
        match=mystr.findall(str(string))
        if len(match)>0:
            return match[0][9:-4]
            
events['tr_decoded'] = events['parameters.tr'].map(lambda x: str_decode(x))

print(events.shape)
# Removing events with tr_decoded value 'Not Found'
final_events=events[~(events['tr_decoded']=='Not Found')]


# generating features 
final_events['event']=final_events['tr_decoded'].map(lambda x: search('"event":".*","f',x))
final_events['converted']=final_events['event'].map(lambda x: 1 if x=='purchase' else 0)
final_events.timestamp=pd.to_datetime(final_events.timestamp)
final_events.to_csv('new_final_data.csv')


# Finding users from web who do not have 'begin_checkout' event
# Will first use 'id' column to use '/osp' as trigger event
web_order_trigger=final_events[(final_events.id=='/osp') & (final_events['headers.x-client-type']=='web')]

# Counts of events generated
# counts=final_events['event'].value_counts().to_frame()
# counts.to_csv('filter_events.csv')




# Grouping by guest-id
per_user_conversion=(final_events.groupby('headers.x-guest-id')['converted'].mean()*100).to_frame()
per_user_conversion['purchased']=per_user_conversion['converted'].map(lambda x:1 if x>0 else 0)

# Percentage of users getting converted
print(round(per_user_conversion['converted'].mean()*100,2),'percent of users converted')

# Grouping by session-id
per_session_conversion=(final_events.groupby('headers.x-session-id')['converted'].mean()*100).to_frame()

per_session_conversion.sort_values('converted',ascending=False).to_csv('per_session_conversion.csv')

# Taking top 5 session id's
# top5_session_conversion=per_session_conversion.sort_values('converted',ascending=False).index[:5]
# print(top5_session_conversion)
# event_check=pd.DataFrame(columns=final_events.columns)
# for i in top5_session_conversion:
#     guest_id=final_events[final_events['headers.x-session-id']==i].reset_index().loc[0,'headers.x-guest-id']
#     sorted_time=final_events[final_events['headers.x-guest-id']==guest_id].sort_values('timestamp')
#     # print(event_check.columns)
#     print(sorted_time['headers.x-guest-id'].value_counts())
#     # print(sorted_time.columns)
#     event_check=pd.concat([event_check,sorted_time ], ignore_index=True)

# print(event_check)
# event_check.to_csv('event_check.csv')

# (final_events.groupby('headers.x-guest-id')['headers.x-client-type'].nunique()).to_frame().to_csv('client.csv')

# Finding users who did not convert
# final_events['begin_checkout']=final_events['event'].map(lambda x: 1 if x=='begin_checkout' else 0)
# # print(final_events[['useridentifier','begin_checkout','purchase']])
# begin_checkout=final_events[final_events['begin_checkout']==1]
# all_checkout_trigger=pd.concat([begin_checkout,web_order_trigger],ignore_index=True)

# purchase=final_events[final_events['converted']==1]
# purchase_web=web_order_trigger[web_order_trigger['converted']==1]
# all_purchase=pd.concat([purchase,purchase_web],ignore_index=True)
# user_id=list(set(all_checkout_trigger.useridentifier)-set(all_purchase.useridentifier))

# all_checkout_trigger.set_index('useridentifier',inplace=True)
# intrst_not_convr=all_checkout_trigger.loc[user_id,:]
# intrst_not_convr=intrst_not_convr.groupby(intrst_not_convr.index).first().reset_index()
# intrst_not_convr.to_csv('all_interested_not_converted.csv')