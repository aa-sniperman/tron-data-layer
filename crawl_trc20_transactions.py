import asyncio
from pathlib import Path
from celery_app import celery_app
from celery.schedules import timedelta
from tasks.crawl_trc20_transactions import Trc20TransactionCrawler
from json_loader import load_json

async def crawl_all_trc20_accounts(accounts: list[str]):
    crawler = Trc20TransactionCrawler()
    tasks = [crawler.crawl_transactions(account) for account in accounts]
    await asyncio.gather(*tasks)  # Run all accounts concurrently

@celery_app.task
def crawl_trc20_transactions_task():
    PROJECT_ROOT = Path(__file__).resolve().parents[0]
    accounts_path = PROJECT_ROOT / "tracked-accounts.json"
    accounts = load_json(accounts_path)
    asyncio.run(crawl_all_trc20_accounts(accounts))


@celery_app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(
        timedelta(seconds=40),  # Run every 40 secs
        crawl_trc20_transactions_task.s(),
    )