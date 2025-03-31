from entities.from_transaction import FromTransactionRepo
from entities.to_transaction import ToTransactionRepo
from entities.trc20_transfer import Trc20TransferRepo
from celery_jobs.crawl_trc20_transactions import crawl_all_trc20_accounts
from redis_client import redis_client
import asyncio
from tasks.crawl_from_transactions import FromTransactionCrawler
from adapter.tron_grid_client import tron_grid_client


async def main():
    # redis_client.flushdb()
    await Trc20TransferRepo.create_table()
    await FromTransactionRepo.create_table()
    await ToTransactionRepo.create_table()
    return
    accounts = ["TJ2WnwEM2M4ErJQHeMPFMhQLivv1haXhfs"]
    crawler = FromTransactionCrawler()
    # lastest_tx = await NormalTransactionRepo.get_latest_transaction_by_from(accounts[0])
    # print(lastest_tx)
    res = await tron_grid_client.get_from_txs(account=accounts[0], min_ts=0)
    parsed_txs = [crawler.parse_raw_tx(accounts[0], tx) for tx in res]
    print(len(parsed_txs))
    # await crawl_all_from_accounts(accounts)
    # accounts = ["TEY2y92rntFnqwzAnw2df1ACUE5JSas3nG"]
    # await crawl_all_trc20_accounts(accounts)
    # tx_id = '02c3faa9073778f42ddb18b84b57f7d62e0ebeb58a95cc91583e1eed36adbbb0'
    # res = await tron_grid_client.get_tx_info(tx_id)
    # print(res)
    # account = 'TU8K561619KfvQQHAusVE1WuzjCMYi1rdR'
    # res = await tron_grid_client.get_trc20_txs(account, 0)
    # print(res[len(res) - 1])
    return
    raw_tx = {
        "ret": [{"contractRet": "SUCCESS", "fee": 6226260}],
        "signature": [
            "a2dd6aa9afd349d1641e0baf0b999e95535ff18a6c2d96c0951f029072438d787e8e449d729b5f97e1c4772e1e385b3d8c70ef393a9b2d623c72a15b50dc38751c"
        ],
        "txID": "17b0af59f9c383d890e1a5acd8d0893e987de1a9733395450e4bf0999edebe37",
        "net_usage": 0,
        "raw_data_hex": "0a02bb8a22082f3b13528c870f134088c3c19bdc325aae01081f12a9010a31747970652e676f6f676c65617069732e636f6d2f70726f746f636f6c2e54726967676572536d617274436f6e747261637412740a154158611af616412a105158432a8a05aa3933ec4a17121541ef22c1b62ba50703145069beec75dd0cfa3436392244a9059cbb0000000000000000000000008829d684dda4186c6c557cf22848a7c6e7c8d60d00000000000000000000000000000000000000000000000053444835ec58000070b486be9bdc32900180c2d72f",
        "net_fee": 345000,
        "energy_usage": 0,
        "blockNumber": 70695838,
        "block_timestamp": 1742740623000,
        "energy_fee": 5881260,
        "energy_usage_total": 28006,
        "raw_data": {
            "contract": [
                {
                    "parameter": {
                        "value": {
                            "data": "a9059cbb0000000000000000000000008829d684dda4186c6c557cf22848a7c6e7c8d60d00000000000000000000000000000000000000000000000053444835ec580000",
                            "owner_address": "4158611af616412a105158432a8a05aa3933ec4a17",
                            "contract_address": "41ef22c1b62ba50703145069beec75dd0cfa343639",
                        },
                        "type_url": "type.googleapis.com/protocol.TriggerSmartContract",
                    },
                    "type": "TriggerSmartContract",
                }
            ],
            "ref_block_bytes": "bb8a",
            "ref_block_hash": "2f3b13528c870f13",
            "expiration": 1742740677000,
            "fee_limit": 100000000,
            "timestamp": 1742740620084,
        },
        "internal_transactions": [],
    }

    parsed_tx = crawler.parse_raw_tx(account=accounts[0], raw_tx=raw_tx)
    print(parsed_tx)


if __name__ == "__main__":
    # redis_client.flushall()
    asyncio.run(main())
