from entities.types import NormalTransactionType
from entities.from_transaction import FromTransaction, FromTransactionRepo
from adapter.tron_grid_client import tron_grid_client
from adapter.utils import TronUtils
from tasks.base_crawler import BaseTransactionCrawler  # Import the base class

class FromTransactionCrawler(BaseTransactionCrawler):
    @property
    def repo(self):
        return FromTransactionRepo

    @property
    def redis_key(self) -> str:
        return "from_latest_ts"

    async def get_latest_transaction(self, account: str):
        return await self.repo.get_latest_transaction_by_from(account)

    async def _insert_transactions(self, transactions: list[FromTransaction]):
        await self.repo.insert_transactions(transactions)

    async def _fetch_transactions(self, account: str, min_ts: int) -> list:
        return await tron_grid_client.get_from_txs(account, min_ts)

    def parse_raw_tx(self, account: str, raw_tx) -> FromTransaction:
        try:
            tx_type = raw_tx["raw_data"]["contract"][0]["type"]
            if tx_type == NormalTransactionType.TRANSFER_ASSET_CONTRACT:
                return None
            parameter_value = raw_tx["raw_data"]["contract"][0]["parameter"]["value"]
            return FromTransaction(
                status=raw_tx["ret"][0]["contractRet"],
                total_fee=raw_tx["ret"][0]["fee"],
                value=parameter_value.get("amount") or parameter_value.get("call_value") or 0,  # Default to 0 if missing
                tx_id=raw_tx["txID"],
                type=tx_type,
                block_number=raw_tx["blockNumber"],
                block_timestamp=raw_tx["block_timestamp"],
                from_address=account,
                to_address=TronUtils.from_hex_address(
                    parameter_value.get("to_address") or parameter_value.get("contract_address")
                ),
            )
        except KeyError as e:
            print(f"KeyError parsing transaction for {account}: Missing key {e}")
        except Exception as e:
            print(f"Error parsing transaction for {account}: {e}")
        return None
