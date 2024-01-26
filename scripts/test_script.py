from database import AltoCrateDB
import pandas as pd
import requests
import plotly.graph_objects as go

# Step 1: Query data from database
cratedb = AltoCrateDB()

filters = {
    'timestamp': {
        '>=': '',
        '<': ''
    },
    'datapoint': {
        'IN': []
    }
}
data = cratedb.query_data(table_name='', filters=filters)
df = pd.DataFrame(data)


# Step 2: Preprocess data for model inference
message = {}

# Step 3: Send the message to Flask server for model inference using requests
url = 'http://localhost:5000/predict'  # Replace with your Flask server URL
headers = {'content-type': 'application/json'}
response = requests.post(url, json=message, headers=headers)

# Step 4: Process the response
if response.status_code == 200:
    result = response.json()
    print(result)
else:
    print('Model inference failed with status code:', response.status_code)


# Step 5: Get the response and plot between the y_pred and y_true
y_pred = result['prediction']
y_true = df['temperature']

fig = go.Figure()
fig.add_trace(go.Scatter(x=df['timestamp'], y=y_pred, name='y_pred'))
fig.add_trace(go.Scatter(x=df['timestamp'], y=y_true, name='y_true'))
fig.show()