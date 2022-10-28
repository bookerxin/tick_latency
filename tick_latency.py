
# Data on drive - "tcaFixnetTicks"

from dash import Dash, dcc, html
from datetime import datetime
import plotly.express as px
import pandas as pd
import csv

def epoch_convert_and_format(x, return_time=True):
    # Divide by 1m otherwise cant convert to datetime
    time = str(datetime.fromtimestamp(int(row[row.index(item)].split(':')[1]) / 1_000_000))

    if return_time == True: # Return the time formatted. Every second
        time = time.split(' ')[1]
        time = time.split('.')[0]
        return time

    else: # Returns seconds and ms as (10.627) format
        time = time.split(' ')[1]
        time = time.split(':')[2]
        return float(time)

# Dash Init

app = Dash(__name__)

# CSV

data = {'time': [], 'eventTime': [], 'tickTime': []}


with open('/home/anthoy/code/python/dataTest/data/tcaFixnetTicks.log') as file:

    file_reader = csv.reader(file)

    iteration = 0

    for row in file_reader:

        iteration += 1

        # print(row[0])

        for item in row: # Add vendor it came from as two lines wip

            if 'EVENT_TIME' in item:
                data['time'].append(epoch_convert_and_format(item))
                data['eventTime'].append(epoch_convert_and_format(item, False))

            if 'tick_stamp' in item:
                data['tickTime'].append(epoch_convert_and_format(item, False))

            if 'tickSource' in item:
                pass

        # if iteration == 100_000:
        #     break

# Dataframe

df = pd.DataFrame(data)

df.sort_values(by=['time'], inplace=True)

df['latency_ms'] = abs(df['eventTime'] - df['tickTime'])

# Time uniques

times = df['time'].unique()

result_df = pd.DataFrame({ 'time': [], 'max_latency': [] })

# For each time, find max latency

for time in times:
    max_latency = max(df.loc[df['time'] == time, 'latency_ms'])

    # drop from original for faster searching?
    df.drop(df[df['time'] == time].index, inplace=True)

    temp = pd.DataFrame([{'time': time, 'max_latency': max_latency}])

    result_df = pd.concat([result_df, temp])

# Dash - Plotly

fig = px.line( result_df, x='time', y='max_latency',
               labels={
                   'time': 'Time',
                   'max_latency': 'Latency (ms)'
               })

# Layout

app.layout = html.Div(children=[
    html.H1(children='Attempt'),

    html.P(children='Latency plot'),

    dcc.Graph(
        id='latency_graph',
        figure=fig
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)

