from pydantic import BaseModel
from typing import Optional
from clickhouse import get_async_ch_client
from entities.types import NormalTransactionType

class FromTransaction(BaseModel):
    status: str
    tx_id: str
    internal_tx_id: Optional[str] = None  # New optional field
    value: int
    total_fee: int
    block_number: int
    block_timestamp: int
    from_address: str
    to_address: str
    type: NormalTransactionType

    def to_clickhouse_dict(self):
        """Converts the object to a dictionary formatted for ClickHouse insertion."""
        return {
            "status": self.status,
            "tx_id": self.tx_id,
            "internal_tx_id": self.internal_tx_id or "",  # Handle None case
            "value": self.value,
            "total_fee": self.total_fee,
            "block_number": self.block_number,
            "block_timestamp": self.block_timestamp,
            "from": self.from_address,
            "to": self.to_address,
            "type": self.type.value,
        }

    @classmethod
    def from_clickhouse_tuple(cls, row: tuple) -> "FromTransaction":
        """Converts a ClickHouse query result tuple into a NormalTransaction instance."""
        columns = ["status", "tx_id", "internal_tx_id", "total_fee", "value", 
                   "block_number", "block_timestamp", "from_address", "to_address", "type"]

        data_dict = dict(zip(columns, row))

        # Convert transaction type from string to Enum if necessary
        if isinstance(data_dict["type"], str):
            data_dict["type"] = NormalTransactionType(data_dict["type"])

        return cls(**data_dict)

class FromTransactionRepo:
    @staticmethod
    async def create_table():
        """Creates the from_transaction table if it doesn't exist."""
        query = """
        CREATE TABLE IF NOT EXISTS from_transaction (
            status String,
            tx_id String,
            internal_tx_id String DEFAULT '',  -- Added new field
            value UInt64,
            total_fee UInt64,
            block_number UInt64,
            block_timestamp UInt64,
            `from` String,
            `to` String,
            type String
        ) ENGINE = MergeTree()
        ORDER BY (block_timestamp, tx_id);
        """
        client = await get_async_ch_client()
        await client.command(query)

    @staticmethod
    async def insert_transactions(transactions: list[FromTransaction]):
        """Inserts transaction records using ClickHouse's `insert` method."""
        client = await get_async_ch_client()

        data = [
            [
                str(tx.status),
                str(tx.tx_id),
                str(tx.internal_tx_id or ""),  # Handle None case
                int(tx.total_fee),
                int(tx.value),
                int(tx.block_number),
                int(tx.block_timestamp),
                str(tx.from_address),
                str(tx.to_address),
                str(tx.type.value)
            ]
            for tx in transactions
        ]

        await client.insert(
            "from_transaction",
            data,
            column_names=[
                "status", "tx_id", "internal_tx_id", "total_fee", "value", 
                "block_number", "block_timestamp", "from", "to", "type"
            ]
        )

    @staticmethod
    async def get_latest_transaction_by_from(from_address: str) -> FromTransaction | None:
        """Retrieves the latest normal transaction for a given 'from' address."""
        query = """
        SELECT status, tx_id, internal_tx_id, total_fee, value, block_number, block_timestamp, `from`, `to`, type
        FROM from_transaction
        WHERE from = %(from_address)s
        ORDER BY block_timestamp DESC
        LIMIT 1
        """
        client = await get_async_ch_client()
        results = await client.query(query, parameters={"from_address": from_address})

        rows = results.result_rows
        if rows and len(rows) > 0:
            return FromTransaction.from_clickhouse_tuple(rows[0])
        return None

    @staticmethod
    async def get_latest_transaction_by_to(to_address: str) -> FromTransaction | None:
        """Retrieves the latest normal transaction for a given 'to' address."""
        query = """
        SELECT status, tx_id, internal_tx_id, total_fee, value, block_number, block_timestamp, `from`, `to`, type
        FROM from_transaction
        WHERE to = %(to_address)s
        ORDER BY block_timestamp DESC
        LIMIT 1
        """
        client = await get_async_ch_client()
        results = await client.query(query, parameters={"to_address": to_address})

        rows = results.result_rows
        if rows and len(rows) > 0:
            return FromTransaction.from_clickhouse_tuple(rows[0])
        return None
