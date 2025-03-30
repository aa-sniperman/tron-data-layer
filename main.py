from entities.normal_transaction import NormalTransactionRepo
from entities.trc20_transfer import Trc20TransferRepo
from crawl_trc20_transactions import crawl_all_trc20_accounts
from redis_client import redis_client
import asyncio
from adapter.tron_grid_client import tron_grid_client

async def main():
    await Trc20TransferRepo.create_table()
    # await NormalTransactionRepo.create_table()
    # accounts = ["TJ2WnwEM2M4ErJQHeMPFMhQLivv1haXhfs"]
    # await crawl_all_from_accounts(accounts)
    accounts = ["TEY2y92rntFnqwzAnw2df1ACUE5JSas3nG"]
    await crawl_all_trc20_accounts(accounts)
    # res = await tron_grid_client.get_trc20_txs(account=accounts[0], min_ts=0)
    # print(res)


if __name__ == "__main__":
    # redis_client.flushall()
    asyncio.run(main())
