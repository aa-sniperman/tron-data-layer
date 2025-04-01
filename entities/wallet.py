from pydantic import BaseModel
from clickhouse import get_async_ch_client
from entities.types import ClusterType


class Cluster(BaseModel):
    cluster: str
    type: ClusterType

    def to_clickhouse_dict(self):
        """
        Converts the object to a dictionary formatted for ClickHouse insertion.
        """
        return {
            "type": self.type,
            "cluster": self.cluster,
        }

    @classmethod
    def from_clickhouse_tuple(cls, row: tuple) -> "Cluster":
        """
        Converts a ClickHouse query result tuple into a Cluster instance.
        """
        columns = [
            "type",
            "cluster",
        ]
        data_dict = dict(zip(columns, row))

        return cls(**data_dict)


class Wallet(BaseModel):
    address: str
    cluster: str

    def to_clickhouse_dict(self):
        """
        Converts the object to a dictionary formatted for ClickHouse insertion.
        """
        return {
            "address": self.address,
            "cluster": self.cluster,
        }

    @classmethod
    def from_clickhouse_tuple(cls, row: tuple) -> "Wallet":
        """
        Converts a ClickHouse query result tuple into a Wallet instance.
        """
        columns = [
            "address",
            "cluster",
        ]
        data_dict = dict(zip(columns, row))

        return cls(**data_dict)


class WalletRepo:
    @staticmethod
    async def create_wallet_table():
        """Creates the wallet table if it doesn't exist."""
        query = """
        CREATE TABLE IF NOT EXISTS wallet (
            address String,
            cluster String,
        ) ENGINE = MergeTree()
        ORDER BY (cluster, address);
        """
        client = await get_async_ch_client()
        await client.command(query)

    @staticmethod
    async def insert_wallets(wallets: list[Wallet]):
        """
        Inserts wallet records using ClickHouse's `insert` method.
        """
        client = await get_async_ch_client()

        data = [[str(w.address), str(w.cluster)] for w in wallets]

        await client.insert(
            "wallet",
            data,
            column_names=[
                "address",
                "cluster",
            ],
        )

    @staticmethod
    async def get_cluster_accounts(cluster: str):
        """
        Get all wallet addresses of a cluster
        """
        client = await get_async_ch_client()

        return await client.query(
            """
            SELECT address
            FROM wallet
            WHERE cluster = %(cluster)s
            """,
            {"cluster": cluster},
        )

    @staticmethod
    async def create_cluster_table():
        """Creates the cluster table if it doesn't exist."""
        query = """
        CREATE TABLE IF NOT EXISTS cluster (
            type String,
            cluster String,
        ) ENGINE = MergeTree()
        ORDER BY (cluster);
        """
        client = await get_async_ch_client()
        await client.command(query)

    @staticmethod
    async def insert_clusters(clusters: list[Cluster]):
        """
        Inserts clusters using ClickHouse's `insert` method.
        """
        client = await get_async_ch_client()

        data = [[str(c.type), str(c.cluster)] for c in clusters]

        await client.insert(
            "cluster",
            data,
            column_names=[
                "type",
                "cluster",
            ],
        )

    @staticmethod
    async def get_all_cluster_names():
        """
        Get all cluster names
        """
        client = await get_async_ch_client()

        return await client.query(
            """
            SELECT cluster
            FROM cluster
            """
        )
