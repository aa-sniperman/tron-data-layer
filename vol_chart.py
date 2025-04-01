import datetime
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from clickhouse import get_sync_ch_client
from consts import token_configs, ROUTER_ADDRESS
from tracked_accounts import tracked_accounts
from plotly.subplots import make_subplots

# Initialize Dash app
app = dash.Dash(__name__)

accounts = tracked_accounts

# Function to fetch hourly data from ClickHouse
def fetch_hourly_data(query, params=None):
    client = get_sync_ch_client()
    result = client.query(query, parameters=params or {})
    return result.result_rows  # Returns list of (hour, value)

# Main function that handles data processing
def get_data(token: str, max_timestamp: int):
    token_config = token_configs[token]

    # Fetch hourly volume data
    hourly_volume = fetch_hourly_data(
        """
        SELECT toStartOfHour(toDateTime64(block_timestamp / 1000, 3)) AS hour, SUM(value) AS volume
        FROM (
            SELECT block_timestamp, MAX(value) as value
            FROM from_transaction
            WHERE `from` IN %(addresses)s AND `to` = %(router_address)s
            AND block_timestamp <= %(max_timestamp)s
            AND status = 'SUCCESS'
            GROUP BY block_timestamp
            UNION ALL
            SELECT block_timestamp, MAX(value) as value
            FROM to_transaction
            WHERE `from` = %(router_address)s AND `to` IN %(addresses)s
            AND block_timestamp <= %(max_timestamp)s
            AND status = 'SUCCESS'
            GROUP BY block_timestamp
        )
        GROUP BY hour
        ORDER BY hour
        """, {"addresses": accounts, "router_address": ROUTER_ADDRESS, "max_timestamp": max_timestamp}
    )

    # Scale and accumulate volume data (scaled by 1e6 for each record)
    hourly_volume = [(hour, volume / 1e6) for hour, volume in hourly_volume]
    accumulated_volume = []
    total_volume = 0
    for hour, volume in hourly_volume:
        total_volume += volume
        accumulated_volume.append((hour, total_volume))

    # Fetch hourly gas data
    hourly_gas = fetch_hourly_data(
        """
        SELECT toStartOfHour(toDateTime64(block_timestamp / 1000, 3)) AS hour, SUM(total_fee) AS gas_used
        FROM (
            SELECT block_timestamp, MAX(total_fee) as total_fee
            FROM from_transaction
            WHERE `from` IN %(addresses)s
            AND block_timestamp <= %(max_timestamp)s
            GROUP BY block_timestamp
        )
        GROUP BY hour
        ORDER BY hour
        """, {"addresses": accounts, "max_timestamp": max_timestamp}
    )

    # Scale and accumulate gas data (scaled by 1e6 for each record)
    hourly_gas = [(hour, gas / 1e6) for hour, gas in hourly_gas]
    accumulated_gas = []
    total_gas = 0
    for hour, gas in hourly_gas:
        total_gas += gas
        accumulated_gas.append((hour, total_gas))

    return hourly_volume, accumulated_volume, hourly_gas, accumulated_gas


# Cache to store previously fetched data
cache = {}

# Function to check if data is cached
def get_cached_data(token, max_timestamp):
    cache_key = f"{token}_{max_timestamp}"
    if cache_key in cache:
        return cache[cache_key]
    else:
        return None

# Define the layout with three graphs and separate controls for each
app.layout = html.Div([
    html.Div(children=[
        dcc.Graph(id='hourly-volume-graph'),
    ], style={'padding': 10, 'flex': 1}),
    
    html.Div(children=[
        dcc.Graph(id='hourly-gas-graph'),
    ], style={'padding': 10, 'flex': 1}),
    
    # html.Div(children=[
    #     dcc.Graph(id='accumulated-graph'),
    # ], style={'padding': 10, 'flex': 1}),

    dcc.Interval(
        id='graph-update',
        interval=1000 * 60,  # 1 minute update interval (adjust as needed)
        n_intervals=0
    )
], style={'display': 'flex', 'flexDirection': 'column'})


@app.callback(
    Output('hourly-volume-graph', 'figure'),
    Input('graph-update', 'n_intervals')
)
def update_hourly_volume_graph(n_intervals):
    token = 'sunana'  # Change this if needed
    max_timestamp = int(datetime.datetime.now().timestamp() * 1000)
    cached_data = get_cached_data(token, max_timestamp)
    
    if cached_data is None:
        # Fetch the data asynchronously
        hourly_volume, accumulated_volume, hourly_gas, accumulated_gas = get_data(token, max_timestamp)
        # Store the result in the cache
        cache[f"{token}_{max_timestamp}"] = (hourly_volume, accumulated_volume, hourly_gas, accumulated_gas)
    else:
        hourly_volume, accumulated_volume, _, _ = cached_data

    hours_v, volumes = zip(*hourly_volume)
    _, acc_volumes = zip(*accumulated_volume)

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add hourly volume (bars) to the primary y-axis (left)
    fig.add_trace(
        go.Bar(x=hours_v, y=volumes, name='Hourly Volume', marker_color='blue'),
        secondary_y=False
    )

    # Add accumulated volume (line) to the secondary y-axis (right)
    fig.add_trace(
        go.Scatter(x=hours_v, y=acc_volumes, mode='lines', name='Accumulated Volume', line=dict(color='red')),
        secondary_y=True
    )

    # Update layout
    fig.update_layout(
        title='Hourly Trading Volume',
        xaxis_title='Hour',
        yaxis=dict(title='Hourly Volume', side='left', showgrid=False),
        yaxis2=dict(title='Accumulated Volume', overlaying='y', side='right', showgrid=False),
        legend=dict(x=0, y=1),
        showlegend=False
    )

    return fig


# Callback for Hourly Gas Graph
@app.callback(
    Output('hourly-gas-graph', 'figure'),
    Input('graph-update', 'n_intervals')
)
def update_hourly_gas_graph(n_intervals):
    token = 'sunana'  # Change this if needed
    max_timestamp = int(datetime.datetime.now().timestamp() * 1000)
    cached_data = get_cached_data(token, max_timestamp)
    if cached_data is None:
        # Fetch the data asynchronously
        hourly_volume, accumulated_volume, hourly_gas, accumulated_gas = get_data(token, max_timestamp)
        # Store the result in the cache
        cache[f"{token}_{max_timestamp}"] = (hourly_volume, accumulated_volume, hourly_gas, accumulated_gas)
    else:
        _, _, hourly_gas, accumulated_gas = cached_data

    hours_g, gas = zip(*hourly_gas)
    _, acc_gas = zip(*accumulated_gas)

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add hourly volume (bars) to the primary y-axis (left)
    fig.add_trace(
        go.Bar(x=hours_g, y=gas, name='Hourly Gas Usage', marker_color='red'),
        secondary_y=False
    )

    # Add accumulated volume (line) to the secondary y-axis (right)
    fig.add_trace(
        go.Scatter(x=hours_g, y=acc_gas, mode='lines', name='Accumulated Gas Usage', line=dict(color='blue')),
        secondary_y=True
    )

    # Update layout
    fig.update_layout(
        title='Hourly Gas Usage',
        xaxis_title='Hour',
        yaxis=dict(title='Hourly Gas Usage', side='left', showgrid=False),
        yaxis2=dict(title='Accumulated Gas Usage', overlaying='y', side='right', showgrid=False),
        legend=dict(x=0, y=1),
        showlegend=False
    )

    return fig

def update_accumulated_graph(n_intervals):
    token = 'sunana'  # Change this if needed
    max_timestamp = int(datetime.datetime.now().timestamp() * 1000)
    cached_data = get_cached_data(token, max_timestamp)
    if cached_data is None:
        # Fetch the data asynchronously
        hourly_volume, accumulated_volume, hourly_gas, accumulated_gas = get_data(token, max_timestamp)
        # Store the result in the cache
        cache[f"{token}_{max_timestamp}"] = (hourly_volume, accumulated_volume, hourly_gas, accumulated_gas)
    else:
        _, accumulated_volume, _, accumulated_gas = cached_data

    # Find common timestamps
    hours_vol, acc_volumes = zip(*accumulated_volume)
    hours_gas, acc_gas = zip(*accumulated_gas)

    common_hours = sorted(set(hours_vol) & set(hours_gas))  # Intersection of timestamps

    # Filter accumulated data to include only common timestamps
    acc_volumes = [v for h, v in accumulated_volume if h in common_hours]
    acc_gas = [g for h, g in accumulated_gas if h in common_hours]

    figure = {
        'data': [
            go.Scatter(x=common_hours, y=acc_volumes, mode='lines', name='Accumulated Volume', line=dict(color='blue')),
            go.Scatter(x=common_hours, y=acc_gas, mode='lines', name='Accumulated Gas Used', line=dict(color='red'))
        ],
        'layout': go.Layout(
            title='Accumulated Volume & Gas Usage',
            xaxis={'title': 'Hour'},
            yaxis={'title': 'Accumulated Value'},
            showlegend=True
        )
    }

    return figure


if __name__ == '__main__':
    app.run(debug=True)
