import plotly.graph_objects as go
import pandas as pd
import os

if not os.path.exists("images"):
   os.mkdir("images")

data=pd.read_csv('top_5_users_data.csv')

# ['e8610bbf-b03e-43f7-a6d3-f9e6c6348d01', '60a5e187cfb72611e06f5d2d',
# '9c2a93d4-70a2-4e0f-a328-9db4d52c54a7', '60a5798e0da2c6793191dca1','60a5daf60da2c6793191eac5']
# '4d96a0ca-262d-4f0f-b699-30817466a9f0'
data.timestamp=pd.to_datetime(data.timestamp)

user_data=data[data['useridentifier']=='9c2a93d4-70a2-4e0f-a328-9db4d52c54a7'].sort_values('timestamp')

user_graph=user_data[~(user_data.event.isna())]

fig = go.Figure()
fig.add_trace(go.Scatter(x=user_graph['timestamp'], y=user_graph['event'], mode='lines+markers',name='lines+markers'))

fig.write_image("images/user1.svg")