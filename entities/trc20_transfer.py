from pydantic import BaseModel
from clickhouse import get_async_ch_client

class Trc20Transfer(BaseModel):
    tx_id: str
    token_address: str
    block_timestamp: int  # UInt64 in ClickHouse maps to Python int
    from_address: str  # Avoiding Python keyword conflict
    to_address: str  # Avoiding ambiguity
    value: str

    def to_clickhouse_dict(self):
        """
        Converts the object to a dictionary formatted for ClickHouse insertion.
        """
        return {
            "tx_id": self.tx_id,
            "token_address": self.token_address,
            "block_timestamp": self.block_timestamp,
            "from": self.from_address,
            "to": self.to_address,
            "value": self.value
        }

    @classmethod
    def from_clickhouse_tuple(cls, row: tuple) -> "Trc20Transfer":
        """
        Converts a ClickHouse query result tuple into a Trc20Transfer instance.
        """
        columns = ["tx_id", "token_address", "block_timestamp", "from_address", "to_address", "value"]
        data_dict = dict(zip(columns, row))
        return cls(**data_dict)

class Trc20TransferRepo:
    @staticmethod
    async def create_table():
        """Creates the trc20_transfer table if it doesn't exist."""
        query = """
        CREATE TABLE IF NOT EXISTS trc20_transfer (
            tx_id String,
            token_address String,
            block_timestamp UInt64,
            `from` String,
            `to` String,
            value Decimal(76, 0)
        ) ENGINE = MergeTree()
        ORDER BY (block_timestamp, from);
        """
        client = await get_async_ch_client()
        await client.command(query)

    @staticmethod
    async def insert_transactions(transfers: list[Trc20Transfer]):
        """
        Inserts TRC-20 transfer records using ClickHouse's `insert` method.
        """
        client = await get_async_ch_client()

        data = [
            [
                str(tx.tx_id),
                str(tx.token_address),
                int(tx.block_timestamp),
                str(tx.from_address),
                str(tx.to_address),
                str(tx.value)
            ]
            for tx in transfers
        ]

        await client.insert(
            "trc20_transfer",
            data,
            column_names=["tx_id", "token_address", "block_timestamp", "from", "to", "value"]
        )

    @staticmethod
    async def get_latest_transfer_by_account(account: str) -> Trc20Transfer | None:
        """
        Retrieves the latest TRC-20 transfer where either 'from' or 'to' matches the account.
        """
        query = """
        SELECT tx_id, token_address, block_timestamp, `from`, `to`, value
        FROM trc20_transfer
        WHERE from = %(account)s OR to = %(account)s
        ORDER BY block_timestamp DESC
        LIMIT 1
        """
        client = await get_async_ch_client()
        results = await client.query(query, parameters={"account": account})

        rows = results.result_rows
        if rows and len(rows) > 0:
            return Trc20Transfer.from_clickhouse_tuple(rows[0])
        return None