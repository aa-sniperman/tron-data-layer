from abc import ABC, abstractmethod
from redis_client import redis_client

class BaseTransactionCrawler(ABC):
    """
    Base class for transaction crawlers. Handles common logic for fetching,
    storing, and caching transactions.
    """
    
    @property
    @abstractmethod
    def repo(self):
        """Repository for storing transactions"""
        pass

    @property
    @abstractmethod
    def redis_key(self) -> str:
        """Redis key prefix for caching latest timestamp"""
        pass

    @abstractmethod
    async def _fetch_transactions(self, account: str, min_ts: int) -> list:
        """Fetch transactions from external source"""
        pass

    @abstractmethod
    def _parse_raw_tx(self, account: str, raw_tx):
        """Parse raw transaction into a database entity"""
        pass

    @abstractmethod
    async def get_latest_transaction(self, account: str):
        """Abstract method to be implemented by subclasses to fetch the latest transaction from the DB."""
        pass

    @abstractmethod
    async def _insert_transactions(self, transactions: list):
        """Insert parsed transactions into the database."""
        pass
    
    async def _get_account_latest_ts(self, account: str) -> int:
        try:
            latest_ts = redis_client.get(f"{self.redis_key}:{account}")
            if latest_ts is not None:
                return int(latest_ts)
            
            latest_tx = await self.get_latest_transaction(account)
            if latest_tx:
                return int(latest_tx.block_timestamp)
        except Exception as e:
            print(f"Error getting latest timestamp for {account}: {e}")
        
        return 0
    
    async def _store_transactions(self, account: str, transactions: list):
        if not transactions:
            return
        
        await self._insert_transactions(transactions)
        latest_ts = max(tx.block_timestamp for tx in transactions)
        redis_client.set(f"{self.redis_key}:{account}", latest_ts)
    
    async def crawl_transactions(self, account: str):
        print(f"Crawling transactions for account {account}")
        try:
            min_ts = await self._get_account_latest_ts(account)
            raw_txs = await self._fetch_transactions(account, min_ts + 1)
            parsed_txs = [self._parse_raw_tx(account, tx) for tx in raw_txs]
            parsed_txs = [tx for tx in parsed_txs if tx is not None and tx.block_timestamp > min_ts]
            
            await self._store_transactions(account, parsed_txs)
            return parsed_txs
        except Exception as e:
            print(f"Error crawling transactions for {account}: {e}")
        return []
