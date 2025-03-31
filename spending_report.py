import asyncio
import csv
import sys
from clickhouse import get_async_ch_client
from tracked_accounts import tracked_accounts
from consts import token_configs, ROUTER_ADDRESS, SUNPUMP_ADDRESS

accounts = tracked_accounts
# accounts = list(["TJ2WnwEM2M4ErJQHeMPFMhQLivv1haXhfs"])

async def fetch_metric(query, params=None):
    client = await get_async_ch_client()
    result = await client.query(query, parameters=params or {})
    return result.result_rows[0][0] if result.result_rows else 0

async def fetch_and_print_metric(query, params=None):
    client = await get_async_ch_client()
    result = await client.query(query, parameters=params or {})
    rows = result.result_rows

    # Print all transactions
    for row in rows:
        print(row)

    # Sum the values
    return sum(row[1] for row in rows) if rows else 0

async def main(token: str, max_timestamp: int):
    token_config = token_configs[token]

    excluded_accounts = list([
        ROUTER_ADDRESS,
        token_config.address,
        token_config.pair,
        SUNPUMP_ADDRESS
    ])
    
    metrics = {}
    
    # 1️⃣ Total Gas Used
    metrics["Total Gas Used"] = await fetch_metric(
        """
        SELECT SUM(total_fee)
        FROM (
            SELECT tx_id, MAX(total_fee) as total_fee
            FROM from_transaction
            WHERE `from` IN %(addresses)s AND block_timestamp <= %(max_timestamp)s
            GROUP BY tx_id
        )
        """, {"addresses": accounts, "max_timestamp": max_timestamp}
    )
    
    # 2️⃣ Total TRX Sent (Excluding PAIR_ADDRESS & ROUTER_ADDRESS)
    metrics["Total TRX Sent"] = await fetch_metric(
        """
        SELECT SUM(value)
        FROM (
            SELECT tx_id, MAX(value) as value
            FROM from_transaction
            WHERE `from` IN %(addresses)s 
            AND `to` NOT IN %(excluded)s
            AND block_timestamp <= %(max_timestamp)s
            AND status = 'SUCCESS'
            GROUP BY tx_id
        )
        """, {"addresses": accounts, "excluded": excluded_accounts, "max_timestamp": max_timestamp}
    )

    # 3️⃣ Total TRX Received (Excluding PAIR_ADDRESS & ROUTER_ADDRESS)
    metrics["Total TRX Received"] = await fetch_metric(
        """
        SELECT SUM(value)
        FROM (
            SELECT tx_id, MAX(value) as value
            FROM to_transaction
            WHERE `to` IN %(addresses)s 
            AND `from` NOT IN (%(excluded)s)
            AND block_timestamp <= %(max_timestamp)s
            AND status = 'SUCCESS'
            AND `type` != 'UnDelegateResourceContract'
            GROUP BY tx_id
        )
        """, {"addresses": accounts, "excluded": excluded_accounts, "max_timestamp": max_timestamp}
    )

    
    # 4️⃣ Total Sunana Sold
    metrics["Total Sunana Sold"] = await fetch_metric(
        """
        SELECT SUM(value)
        FROM (
            SELECT tx_id, MAX(value) as value
            FROM trc20_transfer
            WHERE `from` IN %(addresses)s AND `to` = %(PAIR_ADDRESS)s AND token_address = %(token)s
            AND block_timestamp <= %(max_timestamp)s
            GROUP BY tx_id
        )
        """, {"addresses": accounts, "PAIR_ADDRESS": token_config.pair, "token": token_config.address, "max_timestamp": max_timestamp}
    )
    
    # 5️⃣ Total Sunana Bought
    metrics["Total Sunana Bought"] = await fetch_metric(
        """
        SELECT SUM(value)
        FROM (
            SELECT tx_id, MAX(value) as value
            FROM trc20_transfer
            WHERE `from` = %(PAIR_ADDRESS)s AND `to` IN %(addresses)s AND token_address = %(token)s
            AND block_timestamp <= %(max_timestamp)s
            GROUP BY tx_id
        )
        """, {"addresses": accounts, "PAIR_ADDRESS": token_config.pair, "token": token_config.address, "max_timestamp": max_timestamp}
    )
    
    # 6️⃣ Total TRX Spent on Buys
    metrics["Total TRX Spent on Buys"] = await fetch_metric(
        """
        SELECT SUM(value)
        FROM (
            SELECT tx_id, MAX(value) as value
            FROM from_transaction
            WHERE `from` IN %(addresses)s AND `to` = %(router_address)s AND block_timestamp <= %(max_timestamp)s
            AND status = 'SUCCESS'
            GROUP BY tx_id
        )
        """, {"addresses": accounts, "router_address": ROUTER_ADDRESS, "max_timestamp": max_timestamp}
    )
    
    # 7️⃣ Total TRX Received on Sells
    metrics["Total TRX Received on Sells"] = await fetch_metric(
        """
        SELECT SUM(value)
        FROM (
            SELECT tx_id, MAX(value) as value
            FROM to_transaction
            WHERE `from` = %(router_address)s AND `to` IN %(addresses)s AND block_timestamp <= %(max_timestamp)s
            AND status = 'SUCCESS'
            GROUP BY tx_id
        )
        """, {"addresses": accounts, "router_address": ROUTER_ADDRESS, "max_timestamp": max_timestamp}
    )

    # 8 Total TRX Spent on Curve
    metrics["Total TRX Spent on Curve"] = await fetch_metric(
        """
        SELECT SUM(value)
        FROM (
            SELECT tx_id, MAX(value) as value
            FROM from_transaction
            WHERE `from` IN %(addresses)s AND `to` = %(curve)s AND block_timestamp <= %(max_timestamp)s
            AND status = 'SUCCESS'
            GROUP BY tx_id
        )
        """, {"addresses": accounts, "curve": SUNPUMP_ADDRESS, "max_timestamp": max_timestamp}
    )

    # 9 Total TRX Received on Curve
    metrics["Total TRX Received on Curve"] = await fetch_metric(
        """
        SELECT SUM(value)
        FROM (
            SELECT tx_id, MAX(value) as value
            FROM to_transaction
            WHERE `to` IN %(addresses)s AND `from` = %(curve)s AND block_timestamp <= %(max_timestamp)s
            AND status = 'SUCCESS'
            GROUP BY tx_id
        )
        """, {"addresses": accounts, "curve": SUNPUMP_ADDRESS, "max_timestamp": max_timestamp}
    )
    
    # Export to CSV
    with open(f"makers_report_{max_timestamp}.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Metric", "Value"])
        for metric, value in metrics.items():
            writer.writerow([metric, value])
    
    print("Maker metrics exported to csv")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python spending_report.py <token> <max_timestamp>")
        sys.exit(1)
    
    token = sys.argv[1]
    max_timestamp = int(sys.argv[2])

    asyncio.run(main(token, max_timestamp))
