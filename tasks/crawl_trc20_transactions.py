from entities.trc20_transfer import Trc20TransferRepo, Trc20Transfer
from adapter.tron_grid_client import tron_grid_client
from tasks.base_crawler import BaseTransactionCrawler  # Import the base class

class Trc20TransactionCrawler(BaseTransactionCrawler):
    @property
    def repo(self):
        return Trc20TransferRepo

    @property
    def redis_key(self) -> str:
        return "trc20_latest_ts"

    async def get_latest_transaction(self, account: str):
        return await self.repo.get_latest_transfer_by_account(account)

    async def _insert_transactions(self, transactions: list[Trc20Transfer]):
        await self.repo.insert_transactions(transactions)

    async def _fetch_transactions(self, account: str, min_ts: int) -> list:
        return await tron_grid_client.get_trc20_txs(account, min_ts)

    def _parse_raw_tx(self, account: str, raw_tx) -> Trc20Transfer:
        if raw_tx.get("type") != "Transfer":
            return None
        try:
            return Trc20Transfer(
                tx_id=raw_tx["transaction_id"],
                token_address=raw_tx["token_info"]["address"],
                block_timestamp=raw_tx["block_timestamp"],
                from_address=raw_tx["from"],
                to_address=raw_tx["to"],
                value=raw_tx["value"]
            )
        except KeyError as e:
            print(f"KeyError parsing transaction for {account}: Missing key {e}")
        except Exception as e:
            print(f"Error parsing transaction for {account}: {e}")
        return None  # Return None if parsing fails
