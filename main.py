from entities.normal_transaction import NormalTransactionRepo
from entities.trc20_transfer import Trc20TransferRepo
from celery_jobs.crawl_trc20_transactions import crawl_all_trc20_accounts
from redis_client import redis_client
import asyncio
from adapter.tron_grid_client import tron_grid_client

async def main():
    redis_client.flushdb()
    await Trc20TransferRepo.create_table()
    # await NormalTransactionRepo.create_table()
    # accounts = ["TJ2WnwEM2M4ErJQHeMPFMhQLivv1haXhfs"]
    # await crawl_all_from_accounts(accounts)
    # accounts = ["TEY2y92rntFnqwzAnw2df1ACUE5JSas3nG"]
    # await crawl_all_trc20_accounts(accounts)
    # tx_id = '02c3faa9073778f42ddb18b84b57f7d62e0ebeb58a95cc91583e1eed36adbbb0'
    # res = await tron_grid_client.get_tx_info(tx_id)
    # print(res)
    # account = 'TU8K561619KfvQQHAusVE1WuzjCMYi1rdR'
    # res = await tron_grid_client.get_to_txs(account, 1742744469000)
    # print(res[0])

if __name__ == "__main__":
    # redis_client.flushall()
    asyncio.run(main())
