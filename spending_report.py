import asyncio
import csv
import sys
from pathlib import Path
from clickhouse import get_async_ch_client
from json_loader import load_json

PROJECT_ROOT = Path(__file__).resolve().parents[0]
accounts_path = PROJECT_ROOT / "tracked-accounts.json"
accounts = load_json(accounts_path)

PAIR_ADDRESS = "TJ9g2SzMSH7yV71AtpEjLa4HQyRkSGYHW4"
ROUTER_ADDRESS = "TXF1xDbVGdxFGbovmmmXvBGu8ZiE3Lq4mR"
SUNANA_TOKEN_ADDRESS = "TXme8qsGdorboWFTX3E2XBWkmLNq4h7Kbx"
SUNPUMP_ADDRESS = "TTfvyrAz86hbZk5iDpKD78pqLGgi8C7AAw"
excluded_accounts = list([
    ROUTER_ADDRESS,
    PAIR_ADDRESS,
    SUNPUMP_ADDRESS,
    SUNANA_TOKEN_ADDRESS
])

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

async def main(max_timestamp):
    metrics = {}
    
    # 1️⃣ Total Gas Used
    metrics["Total Gas Used"] = await fetch_metric(
        """
        SELECT SUM(total_fee)
        FROM (
            SELECT tx_id, MAX(total_fee) as total_fee
            FROM normal_transaction
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
            FROM normal_transaction
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
            FROM normal_transaction
            WHERE `to` IN %(addresses)s 
            AND `from` NOT IN (%(excluded)s)
            AND block_timestamp <= %(max_timestamp)s
            AND status = 'SUCCESS'
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
        """, {"addresses": accounts, "PAIR_ADDRESS": PAIR_ADDRESS, "token": SUNANA_TOKEN_ADDRESS, "max_timestamp": max_timestamp}
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
        """, {"addresses": accounts, "PAIR_ADDRESS": PAIR_ADDRESS, "token": SUNANA_TOKEN_ADDRESS, "max_timestamp": max_timestamp}
    )
    
    # 6️⃣ Total TRX Spent on Buys
    metrics["Total TRX Spent on Buys"] = await fetch_metric(
        """
        SELECT SUM(value)
        FROM (
            SELECT tx_id, MAX(value) as value
            FROM normal_transaction
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
            FROM normal_transaction
            WHERE `from` = %(router_address)s AND `to` IN %(addresses)s AND block_timestamp <= %(max_timestamp)s
            AND status = 'SUCCESS'
            GROUP BY tx_id
        )
        """, {"addresses": accounts, "router_address": ROUTER_ADDRESS, "max_timestamp": max_timestamp}
    )
    
    # Export to CSV
    with open(f"makers_report_{max_timestamp}.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Metric", "Value"])
        for metric, value in metrics.items():
            writer.writerow([metric, value])
    
    print("Maker metrics exported to csv")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python analytics/spending.py <max_timestamp>")
        sys.exit(1)
    
    max_timestamp = int(sys.argv[1])
    asyncio.run(main(max_timestamp))
