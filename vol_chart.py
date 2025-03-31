from tracked_accounts import tracked_accounts
import asyncio
import sys
from clickhouse import get_async_ch_client
from consts import token_configs, ROUTER_ADDRESS
import datetime
import matplotlib.pyplot as plt

accounts = tracked_accounts

def plot_data(hourly_volume, accumulated_volume, hourly_gas, accumulated_gas):
    plt.figure(figsize=(12, 6))

    # Plot Hourly Volume
    plt.subplot(2, 2, 1)
    hours_v, volumes = zip(*hourly_volume)
    plt.bar(hours_v, volumes, color='blue', alpha=0.7)
    plt.xlabel("Hour")
    plt.ylabel("Hourly Volume")
    plt.title("Hourly Trading Volume")
    plt.xticks(rotation=45)

    # Plot Hourly Gas Usage
    plt.subplot(2, 2, 2)
    hours_g, gas_used = zip(*hourly_gas)
    plt.bar(hours_g, gas_used, color='red', alpha=0.7)
    plt.xlabel("Hour")
    plt.ylabel("Hourly Gas Used")
    plt.title("Hourly Gas Usage")
    plt.xticks(rotation=45)

    # Find common timestamps
    hours_vol, acc_volumes = zip(*accumulated_volume)
    hours_gas, acc_gas = zip(*accumulated_gas)

    common_hours = sorted(set(hours_vol) & set(hours_gas))  # Intersection of timestamps

    # Filter accumulated data to include only common timestamps
    acc_volumes = [v for h, v in accumulated_volume if h in common_hours]
    acc_gas = [g for h, g in accumulated_gas if h in common_hours]

    # Merged Accumulated Volume & Gas Usage
    plt.subplot(2, 1, 2)
    plt.plot(common_hours, acc_volumes, linestyle='-', color='blue', label="Accumulated Volume")
    plt.plot(common_hours, acc_gas, linestyle='-', color='red', label="Accumulated Gas Used")
    plt.xlabel("Hour")
    plt.ylabel("Accumulated Value")
    plt.title("Accumulated Volume & Gas Usage")
    plt.xticks(rotation=45)
    plt.legend()

    plt.tight_layout()
    plt.show()

async def fetch_hourly_data(query, params=None):
    client = await get_async_ch_client()
    result = await client.query(query, parameters=params or {})
    return result.result_rows  # Returns list of (hour, value)
 
async def main(token: str, max_timestamp: int):
    token_config = token_configs[token]

    hourly_volume = await fetch_hourly_data(
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

    accumulated_volume = []
    total_volume = 0
    for hour, volume in hourly_volume:
        total_volume += volume
        accumulated_volume.append((hour, total_volume))

    hourly_gas = await fetch_hourly_data(
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

    accumulated_gas = []
    total_gas = 0
    for hour, gas in hourly_gas:
        total_gas += gas
        accumulated_gas.append((hour, total_gas))

    plot_data(
        hourly_volume=hourly_volume,
        accumulated_volume=accumulated_volume,
        hourly_gas=hourly_gas,
        accumulated_gas=accumulated_gas
    )

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python vol_chart.py <token> <max_timestamp>")
        sys.exit(1)

    token = sys.argv[1]
    max_timestamp = int(sys.argv[2])
    asyncio.run(main(token, max_timestamp))
