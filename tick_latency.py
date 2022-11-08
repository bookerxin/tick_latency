from dash import Dash, dcc, html, Input, Output
import plotly.express as px
from decimal import Decimal
import numpy as np
import pandas as pd
import os.path
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
    'background': 'rgba(196, 225, 255, 0.6)',
    'plot_bg': 'rgba(247, 251, 255, 1)',
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
                data['tickSource'].append(item.split(':')[1])

            if 'EVENT_TIME' in item:
                data['appTime'].append(row[0].split(',')[0])
                data['eventDateTime'].append(epoch_format(item, True))
                data['eventStamp'].append(epoch_format(item))

            if 'tick_stamp' in item:
                data['tickStamp'].append(epoch_format(item))

        if iteration == 500_000:
            break

# Total Dataframe
total_df = pd.DataFrame(data)
pd.set_option('display.max_rows', 1000)
total_df.sort_values(by=['eventDateTime'], inplace=True)

# Make datetime with microsecond epoch / round eventDateTime to minutes
total_df['eventDateTime'] = pd.to_datetime(total_df['eventDateTime'], origin='unix', unit='us')
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
total_df.drop(total_df[total_df['timeDiff'] > 1500].index, inplace=True)  # 5_000 - 5 seconds
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
        percentile_range = range(50, 101, 5)
        floatTimeDiffs = timeDiffs.apply(lambda row: float(row)).to_numpy()

        # Adding percentiles / We use range 100 as the 99 trigger
        for percentile in percentile_range:
            percentile_value = np.percentile(floatTimeDiffs, percentile)

            if percentile == 100:
                percentile = 99
                percentile_value = np.percentile(floatTimeDiffs, percentile)

            finishedSource = {'time': time, 'max_latency': maxLatency, 'symbol': source, 'value': percentile_value,
                              'percentile': percentile}

            # Concat Row of data to main dataFrames
            finishedSource = pd.DataFrame([finishedSource])
            dataFrames = pd.concat([dataFrames, finishedSource])

# Figures / Percentile / Max
figures = {'percentile_fig': px.line(dataFrames, x='time', y='value', color='symbol', line_group='percentile',
                                     line_shape='spline',
                                     labels={
                                         'time': 'Time (s)',
                                         'value': 'Percentile Latency (ms)',
                                     }),
           'max_fig': px.line(dataFrames, x='time', y='max_latency', color='symbol', line_shape='spline',
                              labels={
                                  'time': 'Time (s)',
                                  'max_latency': 'Max Latency (ms)',
                              })}

# Sort by time
dataFrames.sort_values(by=['time'], inplace=True)

# Dash Layout
app.layout = html.Div(id='main_div', style={'backgroundColor': colors['background'], 'padding': '20px',
                                            'fontFamily': 'Arial'}, children=[

    html.H1(style={'margin': '50px'}, id='plot_header', children=['Latency Plot']),

    html.Br(),

    html.Div(style={'display': 'flex', 'justify-content': 'space-between'}, children=[

        html.Div(style={'display': 'inline-block'}, children=[
            dcc.Checklist(id='percentile_switch', options=[{'label': 'Percentile', 'value': True}])]),

        html.Div(style={'display': 'inline-block'}, id='percentile_checklist', children=[
            dcc.Checklist(id='percentile_options')
        ])

    ]),

    dcc.Graph(id='latency_graph'),

    html.A(html.Button("Download as HTML"), id="download_button"),
])


# Change Chart Mode
@app.callback(
    Output('latency_graph', 'figure'),
    Output('percentile_checklist', 'children'),
    Input('percentile_switch', 'value'),
    Input('percentile_options', 'value'))
def selected_mode(percentile_switch, percentile_options):  # Change to percentile
    if percentile_switch:
        options = dcc.Checklist(id='percentile_options', options=sorted(dataFrames.percentile.unique()),
                                value=[x for x in dataFrames.percentile.unique()])
        fig = figures['percentile_fig']
        fig.update_layout(plot_bgcolor=colors['plot_bg'], paper_bgcolor=colors['background'])
        return fig, options
    fig = figures['max_fig']
    fig.update_layout(plot_bgcolor=colors['plot_bg'], paper_bgcolor=colors['background'])
    return fig, None
def percentile_options(percentile_switch, percentile_options):
    if percentile_options:
        print(percentile_options)


# Download HTML Charts / Encode each figure, and write in bytes to file
@app.callback(
    Output('download_button', 'children'),
    Input('download_button', 'n_clicks'))
def download_graphs(n_clicks):
    if n_clicks:
        if not os.path.isfile('plotly_graph.html'):
            f = open('plotly_graph.html', 'w')
            f.close()
        with(open('plotly_graph.html', 'r+b')) as f:
            for fig in figures.values():
                fig.update_layout(plot_bgcolor=colors['plot_bg'], paper_bgcolor=colors['background'])
                fig.write_html(buffer)  # Not entirely sure how this works, I kinda made it up
            html_bytes = buffer.getvalue().encode()
            f.write(html_bytes)


if __name__ == '__main__':
    app.run_server(debug=True)
