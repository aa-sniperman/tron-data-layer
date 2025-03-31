from entities.types import NormalTransactionType
from entities.to_transaction import (
    ToTransaction,
    ToTransactionRepo
)
from adapter.tron_grid_client import tron_grid_client
from adapter.utils import TronUtils
from tasks.base_crawler import BaseTransactionCrawler  # Import the base class

class ToTransactionCrawler(BaseTransactionCrawler):
    @property
    def repo(self):
        return ToTransactionRepo

    @property
    def redis_key(self) -> str:
        return "to_latest_ts"

    async def get_latest_transaction(self, account: str):
        return await self.repo.get_latest_transaction_by_to(account)

    async def _insert_transactions(self, transactions: list[ToTransaction]):
        await self.repo.insert_transactions(transactions)

    async def _fetch_transactions(self, account: str, min_ts: int) -> list:
        return await tron_grid_client.get_to_txs(account, min_ts)

    def parse_raw_tx(self, account: str, raw_tx) -> ToTransaction:
        try:
            if raw_tx.get("raw_data"):
                tx_type = raw_tx["raw_data"]["contract"][0]["type"]
                if tx_type == NormalTransactionType.TRANSFER_ASSET_CONTRACT:
                    return None
                parameter_value = raw_tx["raw_data"]["contract"][0]["parameter"]["value"]
                return ToTransaction(
                    status=raw_tx["ret"][0]["contractRet"],
                    total_fee=raw_tx["ret"][0]["fee"],
                    value=parameter_value["balance"]
                    if tx_type == NormalTransactionType.UNDELEGATE_RESOURCE_CONTRACT
                    else parameter_value.get("amount", 0),  # Default to 0 if missing
                    tx_id=raw_tx["txID"],
                    type=tx_type,
                    block_number=raw_tx["blockNumber"],
                    block_timestamp=raw_tx["block_timestamp"],
                    from_address=TronUtils.from_hex_address(
                        parameter_value.get("owner_address") or parameter_value.get("contract_address")
                    ),
                    to_address=account,
                    internal_tx_id=raw_tx.get("internal_tx_id"),
                )
            else:
                data = raw_tx["data"]
                return ToTransaction(
                    status="REJECTED" if data.get("rejected") else "SUCCESS",
                    total_fee=0,
                    value=data["call_value"]["_"],
                    tx_id=raw_tx["tx_id"],
                    internal_tx_id=raw_tx.get("internal_tx_id"),
                    type=NormalTransactionType.INTERNAL,
                    block_number=0,
                    block_timestamp=raw_tx["block_timestamp"],
                    from_address=TronUtils.from_hex_address(raw_tx["from_address"]),
                    to_address=account,
                )
        except KeyError as e:
            print(f"KeyError parsing transaction for {account}: Missing key {e}")
        except Exception as e:
            print(f"Error parsing transaction for {account}: {e}")
        return None
