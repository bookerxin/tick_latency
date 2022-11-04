from dash import Dash, dcc, html, Input, Output
from base64 import b64encode
import plotly.io as pio
import plotly.graph_objs as go
import plotly.express as px
from decimal import Decimal
import numpy as np
import pandas as pd
import sys
import csv
import io


# Split at decimal to remove ms
def epoch_format(item, make_seconds=False):
    time = float(str(item).split(':')[1])
    if make_seconds:
        time = (float(str(time).split('.')[0]))
    return time

def minutes_format(row):
    row = str(row).split('.')[0]
    print(row)
    return

def format_ms(row):
    row = str(str(row).split('.')[0]) + 'ms'
    return row

# Dash Init
app = Dash(__name__)

# I/O - For writing to html and download
buffer = io.StringIO()

colors = {
    'background': 'rgba(142, 39, 245, 0.21)',
    'plot_bg': 'rgba(250,250,250,0.8)',
    'text': 'rgb(250,250,250)'}

# CSV
data = {'appTime': [], 'tickSource': [], 'eventDateTime': [], 'eventStamp': [], 'tickStamp': []}

with open('data/AsiaTicks.log_backup') as file:
    file_reader = csv.reader(file)

    iteration = 0

    for row in file_reader:

        iteration += 1

        for item in row:

            if 'tickSource' in item:
                # np.append(data['tickSource'], item.split(':')[1])
                data['tickSource'].append(item.split(':')[1])

            if 'EVENT_TIME' in item:
                # np.append(data['appTime'], item.split(',')[0])
                # np.append(data['eventDateTime'], epoch_format(item, True))
                # np.append(data['eventStamp'], epoch_format(item))

                data['appTime'].append(row[0].split(',')[0])
                data['eventDateTime'].append(epoch_format(item, True))
                data['eventStamp'].append(epoch_format(item))

            if 'tick_stamp' in item:
                # np.append(data['tickStamp'], epoch_format(item))
                data['tickStamp'].append(epoch_format(item))

        if iteration == 100_000:
            break

# Total Dataframe
total_df = pd.DataFrame(data)
pd.set_option('display.max_rows', 1000)
total_df.sort_values(by=['eventDateTime'], inplace=True)

# Make datetime with microsecond epoch / round eventDateTime to minutes
total_df['eventDateTime'] = pd.to_datetime(total_df['eventDateTime'], origin='unix', unit='us')#.apply(lambda row: minutes_format(row))
total_df['eventDateTime'] = total_df['eventDateTime'].dt.round('min')
total_df['eventStamp'] = pd.to_datetime(total_df['eventStamp'], origin='unix', unit='us')
total_df['tickStamp'] = pd.to_datetime(total_df['tickStamp'], origin='unix', unit='us')

# Calculate Time Differences / Total seconds * 1000 to ms
total_df['timeDiff'] = abs(total_df.eventStamp - total_df.tickStamp)
total_df['timeDiff'] = total_df.timeDiff.apply(lambda row: Decimal(row.total_seconds()) * 1000)

# Get years so can check for 1970 fakes
total_df['year'] = total_df['eventDateTime'].apply(lambda row: str(row).split('-')[0])

# Drop unwanted
total_df.drop(total_df[total_df['tickSource'] == 'reuters_heartbeat'].index, inplace=True)
total_df.drop(total_df[total_df['timeDiff'] > 1500].index, inplace=True) # 5_000 - seconds
total_df.drop(total_df[total_df['year'] == '1970'].index, inplace=True)

# Unique Sources
tickSources = [source for source in total_df['tickSource'].unique()]

# Final Dataframe
dataFrames = pd.DataFrame()

# Organise each sources respective data - eg: bloomberg etc.
for source in tickSources:

    # Source data
    sourceTimes = total_df.loc[total_df['tickSource'] == source, 'eventDateTime']
    sourceTimeDifference = total_df.loc[total_df['tickSource'] == source, 'timeDiff']

    # Source dataframe
    source_data = {'eventDateTime': sourceTimes, 'timeDiff': sourceTimeDifference}
    source_df = pd.DataFrame(source_data)

    # For each unique time, get the stamps and find max latency
    for time in source_df['eventDateTime'].unique():

        # Pull time differences within second
        timeDiffs = source_df.loc[source_df['eventDateTime'] == time, 'timeDiff']

        # Latency
        maxLatency = timeDiffs.max()

        # Symbol Format
        if '_' in source:
            source = source.split('_')
            source = ' '.join(source)

        source = source.capitalize()

        # Percentiles
        percentile_range = range(50, 99, 5)
        floatTimeDiffs = timeDiffs.apply(lambda row: float(row)).to_numpy()

        # Final dictionary for concat
        finishedSource = {'time': time, 'latency': maxLatency, 'symbol': source}

        # Adding percentiles
        for percentile in percentile_range:
            percentile_value = np.percentile(floatTimeDiffs, percentile)
            finishedSource[f'P{percentile}'] = percentile_value

        # Concat Row of data to main dataFrames
        finishedSource = pd.DataFrame([finishedSource])
        dataFrames = pd.concat([dataFrames, finishedSource])

# Sort by time
dataFrames.sort_values(by=['time'], inplace=True)
dataFrames.info(memory_usage='deep')

# Figure
fig = px.line(dataFrames, x='time', y='latency', color='symbol',
                      labels={
                          'time': 'Time (s)',
                          'latency': 'Latency (ms)',
                          'percentile': 'P99'
                      })
fig.update_layout(
    plot_bgcolor=colors['plot_bg'],
    paper_bgcolor=colors['background'],
    font_color=colors['text'],
)

# Dash Layout
app.layout = html.Div(style={'backgroundColor': colors['background'], 'padding': '20px'}, children=[
    html.H1(style={'margin': '50px'}, id='plot_header', children=['Latency Plot']),

    html.Br(),

    html.Div(id='percentile_container'),

    dcc.Checklist(['Percentile'], [], id='percentile_toggle'),

    dcc.Slider(id='percentile_slider'),

    dcc.Graph(id='latency_graph'),

    html.A(html.Button('Download'), id='download_button'),

])

@app.callback(
    Output('latency_graph', 'figure'),
    Input('percentile_slider', 'value'),
    Input('percentile_toggle', 'value'))
def percentile_graph(slider_value, toggle_value):
    print(toggle_value)
    if toggle_value:
        fig = px.line(dataFrames, x='time', y=f'P{slider_value}', color='symbol',
                      labels={
                          'time': 'Time (s)',
                          'latency': 'Latency (ms)',
                          'percentile': 'P99'
                      })
        return fig

    fig = px.line(dataFrames, x='time', y='latency', color='symbol',
                      labels={
                          'time': 'Time (s)',
                          'latency': 'Latency (ms)',
                          'percentile': 'P99'
                      })
    return fig

@app.callback(
    Output(component_id='percentile_slider', component_property='min'),
    Output(component_id='percentile_slider', component_property='max'),
    Output(component_id='percentile_slider', component_property='step'),
    Output(component_id='percentile_slider', component_property='value'),
    Input('percentile_toggle', 'value'))
def return_slider_values(value):
    if value:
        slider_min = 50
        slider_max = 99
        slider_step = 5
        slider_value = slider_min
        return slider_min, slider_max, slider_step, slider_value
    return 0, 0, 0, 0

@app.callback(
    Output('download_button', 'children'),
    Input('latency_graph', 'figure'),
    Input('download_button', 'n_clicks'))
def download_graphs(figure, n_clicks):
    fig = go.Figure(figure)
    fig.write_html(buffer)

    html_bytes = buffer.getvalue().encode()
    encoded = b64encode(html_bytes).decode()

    download = html.A(html.Button('Download'), href="data:text/html;base64," + encoded,
                      download='plotly_tick_latency.html')
    return download


if __name__ == '__main__':
    app.run_server(debug=True)
