from dash import Dash, dcc, html, Input, Output
from base64 import b64encode
from datetime import datetime as dt
import plotly.express as px
import numpy as np
import pandas as pd
import sys
import csv
import io


# Filters 1970 Bad values and returns first entry.
# Split at decimal to turn into seconds

def epoch_format(item):
    time = float(str(item).split(':')[1]) / 1_000_000
    time = float(str(time).split('.')[0])
    return time

def make_seconds(row):
    return row.split(':')[2]


# Dash Init
app = Dash(__name__)

# io
buffer = io.StringIO()

colors = {
    # 'background': 'rgba(70,70,70,0.6)',
    'background': 'rgba(142, 39, 245, 0.21)',
    'plot_bg': 'rgba(250,250,250,0.8)',
    'text': 'rgb(250,250,250)'}

# CSV

data = {'appTime': [], 'tickSource': [], 'eventDateTime': [], 'eventStamp': [], 'tickStamp': []}

# with open('/home/anthoy/code/python/dataTest/data/tcaFixnetTicks.log') as file:
with open('data/AsiaTicks.log_backup') as file:
    file_reader = csv.reader(file)

    iteration = 0

    for row in file_reader:

        iteration += 1

        for item in row:

            if 'tickSource' in item:
                data['tickSource'].append(item.split(':')[1])

            if 'EVENT_TIME' in item:
                data['appTime'].append(row[0].split(',')[0])
                data['eventDateTime'].append(dt.fromtimestamp(epoch_format(item)))
                data['eventStamp'].append(dt.fromtimestamp(epoch_format(item)))

            if 'tick_stamp' in item:
                data['tickStamp'].append(dt.fromtimestamp(epoch_format(item)))

        if iteration == 200_000:
            break

# sys.exit()

# Dataframe
df = pd.DataFrame(data)
df.sort_values(by=['eventDateTime'], inplace=True)

# Unique Sources
tickSources = [source for source in df['tickSource'].unique()]

# Final Dataframe
dataFrames = pd.DataFrame()

# Organise each sources respective data - eg: bloomberg etc.
for source in tickSources:

    if source == 'Reuters heartbeat':
        continue

    # Source data
    sourceTimes = df.loc[df['tickSource'] == source, 'eventDateTime']
    sourceEventStamp = df.loc[df['tickSource'] == source, 'eventStamp']
    sourceTickStamp = df.loc[df['tickSource'] == source, 'tickStamp']

    # Source dataframe
    source_data = {'eventDateTime': sourceTimes, 'eventStamp': sourceEventStamp, 'tickStamp': sourceTickStamp}
    source_df = pd.DataFrame(source_data)
    print(source_df)

    # For each unique time, get the stamps and find max latency
    for time in source_df['eventDateTime'].unique():

        timeTimes = source_df.loc[source_df['eventDateTime'] == time, 'eventDateTime']

        # Made into datetime from epoch, MUST BE WRONG
        timeEvents = source_df.loc[source_df['eventDateTime'] == time, 'eventStamp'] # TODO problem, drops all time values here? keeps dates :(
        timeTicks = source_df.loc[source_df['eventDateTime'] == time, 'tickStamp']

        print(timeEvents) # this one
        print(timeTicks)

        sys.exit()

        # Contains all events and ticks for the second
        timeData = pd.DataFrame({'events': timeEvents, 'ticks': timeTicks})

        # Latency
        print(timeData.events)
        latency = max(abs(timeData.events - timeData.ticks))

        # Symbol Format
        if '_' in source:
            source = source.split('_')
            source = ' '.join(source)

        source = source.capitalize()

        finishedSource = pd.DataFrame([{'time': time, 'latency': latency, 'symbol': source}])
        # print(finishedSource)

        # Concat Row for time to main dataFrames
        dataFrames = pd.concat([dataFrames, finishedSource])

# Sort by time
dataFrames.sort_values(by=['time'], inplace=True)

# Percentiles - WIP
percentiles = {}

for source in dataFrames['symbol'].unique():
    source_latency = dataFrames.loc[dataFrames['symbol'] == source, 'latency']
    source_latency = source_latency.to_numpy()
    percentiles[source] = np.percentile(source_latency, 99)

# print(percentiles)

# Make Base Figures
figures = {}

# print(dataFrames['latency'])

for source in tickSources:

    fig = px.line(dataFrames, x='time', y='latency', color='symbol',
                  labels={
                      'time': 'Time',
                      'latency': 'Latency'
                  })

    figures[source] = fig

fig = figures['bloomberg']

fig.update_layout(
    plot_bgcolor=colors['plot_bg'],
    paper_bgcolor=colors['background'],
    font_color=colors['text'],
)

# Dash Layout
app.layout = html.Div(style={'backgroundColor': colors['background'], 'padding': '20px'}, children=[
    html.H1(style={'margin': '50px'}, id='plot_header', children=['Latency Plot']),

    html.Br(),

    html.Label(id='graph_source_name'),

    dcc.Graph(id='latency_graph',
              figure=fig),

    # html.P(id='percentiles', children=f'Percentiles: {percentiles.values()}'),

    # dcc.Loading(id='loading_graph', type='default', children=html.Div(id='loading_graph_output')),

    html.A(html.Button('Download'), id='download_button'),

])


@app.callback(
    Output('download_button', 'children'),
    Input('download_button', 'n_clicks'))
def download_graphs(n_clicks):
    fig.write_html(buffer)

    html_bytes = buffer.getvalue().encode()
    encoded = b64encode(html_bytes).decode()

    download = html.A(html.Button('Download'), href="data:text/html;base64," + encoded,
                      download='plotly_tick_latency.html')

    return download


if __name__ == '__main__':
    app.run_server(debug=True)
