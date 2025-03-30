from enum import Enum
from pydantic import BaseModel
from clickhouse import get_async_ch_client

class NormalTransactionType(str, Enum):
    TRIGGER_SMART_CONTRACT = "TriggerSmartContract"
    UNDELEGATE_RESOURCE_CONTRACT = "UnDelegateResourceContract"
    TRANSFER_CONTRACT = "TransferContract"
    INTERNAL = "Internal"
    TRANSFER_ASSET_CONTRACT = "TransferAssetContract"

class NormalTransaction(BaseModel):
    status: str
    tx_id: str
    value: int
    total_fee: int  # UInt64 in ClickHouse maps to Python int
    block_number: int
    block_timestamp: int  # Uint64 in ClickHouse maps to Python datetime
    from_address: str  # Avoiding Python keyword conflict
    to_address: str  # Avoiding ambiguity
    type: NormalTransactionType  # New field for transaction type

    def to_clickhouse_dict(self):
        """
        Converts the object to a dictionary formatted for ClickHouse insertion.
        """
        return {
            "status": self.status,
            "tx_id": self.tx_id,
            "value": self.value,
            "total_fee": self.total_fee,
            "block_number": self.block_number,
            "block_timestamp": self.block_timestamp,
            "from": self.from_address,
            "to": self.to_address,
            "type": self.type.value,  # Convert Enum to string
        }

    @classmethod
    def from_clickhouse_tuple(cls, row: tuple) -> "NormalTransaction":
        """
        Converts a ClickHouse query result tuple into a NormalTransaction instance.
        """
        columns = ["status", "tx_id", "total_fee", "value", "block_number",
                   "block_timestamp", "from_address", "to_address", "type"]

        # Convert tuple to dictionary
        data_dict = dict(zip(columns, row))

        # Convert transaction type from string to Enum if necessary
        if isinstance(data_dict["type"], str):
            data_dict["type"] = NormalTransactionType(data_dict["type"])

        return cls(**data_dict)

class NormalTransactionRepo:
    @staticmethod
    async def create_table():
        """Creates the normal_transaction table if it doesn't exist."""
        query = """
        CREATE TABLE IF NOT EXISTS normal_transaction (
            status String,
            tx_id String,
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
    async def insert_transactions(transactions: list[NormalTransaction]):
        """
        Inserts transaction records using ClickHouse's `insert` method.
        """
        client = await get_async_ch_client()

        data = [
            [
                str(tx.status),
                str(tx.tx_id),
                int(tx.total_fee),
                int(tx.value),
                int(tx.block_number),
                int(tx.block_timestamp),
                str(tx.from_address),
                str(tx.to_address),
                str(tx.type.value)  # Convert Enum to string for insertion
            ]
            for tx in transactions
        ]

        await client.insert(
            "normal_transaction",
            data,
            column_names=[
                "status", "tx_id", "total_fee", "value", "block_number",
                "block_timestamp", "from", "to", "type"
            ]
        )

    @staticmethod
    async def get_latest_transaction_by_from(from_address: str) -> NormalTransaction | None:
        """
        Retrieves the latest normal transaction for a given 'from' address.
        """
        query = """
        SELECT status, tx_id, total_fee, value, block_number, block_timestamp, from, to, type
        FROM normal_transaction
        WHERE from = %(from_address)s
        ORDER BY block_timestamp DESC
        LIMIT 1
        """
        client = await get_async_ch_client()
        results = await client.query(query, parameters={"from_address": from_address})

        rows = results.result_rows

        if rows and len(rows) > 0:
            return NormalTransaction.from_clickhouse_tuple(rows[0])
        return None

    @staticmethod
    async def get_latest_transaction_by_to(to_address: str) -> NormalTransaction | None:
        """
        Retrieves the latest normal transaction for a given 'to' address.
        """
        query = """
        SELECT status, tx_id, total_fee, value, block_number, block_timestamp, `from`, `to`, type
        FROM normal_transaction
        WHERE to = %(to_address)s
        ORDER BY block_timestamp DESC
        LIMIT 1
        """
        client = await get_async_ch_client()
        results = await client.query(query, parameters={"to_address": to_address})

        rows = results.result_rows
        if rows and len(rows) > 0:
            return NormalTransaction.from_clickhouse_tuple(rows[0])
        return None
