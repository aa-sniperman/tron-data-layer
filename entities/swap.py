from pydantic import BaseModel
from clickhouse import get_async_ch_client

class Swap(BaseModel):
    tx_id: str
    token_in: str
    token_out: str
    block_timestamp: int  # UInt64 in ClickHouse maps to Python int
    from_address: str  # Avoiding Python keyword conflict
    to_address: str  # Avoiding ambiguity
    amount_in: str
    amount_out: str

    def to_clickhouse_dict(self):
        """
        Converts the object to a dictionary formatted for ClickHouse insertion.
        """
        return {
            "tx_id": self.tx_id,
            "token_in": self.token_in,
            "token_out": self.token_out,
            "block_timestamp": self.block_timestamp,
            "from": self.from_address,
            "to": self.to_address,
            "amount_in": self.amount_in,
            "amount_out": self.amount_out
        }

    @classmethod
    def from_clickhouse_tuple(cls, row: tuple) -> "Swap":
        """
        Converts a ClickHouse query result tuple into a Swap instance.
        """
        columns = ["tx_id", "token_in", "token_out", "block_timestamp", "from_address", "to_address", "amount_in", "amount_out"]
        data_dict = dict(zip(columns, row))

        # Convert Decimal to string
        data_dict["amount_in"] = str(data_dict["amount_in"])
        data_dict["amount_out"] = str(data_dict["amount_out"])

        return cls(**data_dict)

class SwapRepo:
    @staticmethod
    async def create_table():
        """Creates the swap table if it doesn't exist."""
        query = """
        CREATE TABLE IF NOT EXISTS swap (
            tx_id String,
            token_in String,
            token_out String,
            block_timestamp UInt64,
            `from` String,
            `to` String,
            amount_in Decimal(76, 0),
            amount_out Decimal(76, 0)
        ) ENGINE = MergeTree()
        ORDER BY (block_timestamp, from)
        """
        client = await get_async_ch_client()
        await client.command(query)

    @staticmethod
    async def insert_transactions(swaps: list[Swap]):
        """
        Inserts swap transaction records using ClickHouse's `insert` method.
        """
        client = await get_async_ch_client()

        data = [
            [
                str(swap.tx_id),
                str(swap.token_in),
                str(swap.token_out),
                int(swap.block_timestamp),
                str(swap.from_address),
                str(swap.to_address),
                str(swap.amount_in),
                str(swap.amount_out)
            ]
            for swap in swaps
        ]

        await client.insert(
            "swap",
            data,
            column_names=["tx_id", "token_in", "token_out", "block_timestamp", "from", "to", "amount_in", "amount_out"]
        )