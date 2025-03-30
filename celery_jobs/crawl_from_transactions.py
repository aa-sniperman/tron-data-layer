import asyncio
from pathlib import Path
from celery_app import celery_app
from celery.schedules import timedelta
from tasks.crawl_from_transactions import FromTransactionCrawler
from json_loader import load_json

async def crawl_all_from_accounts(accounts: list[str]):
    crawler = FromTransactionCrawler()
    tasks = [crawler.crawl_transactions(account) for account in accounts]
    await asyncio.gather(*tasks)  # Run all accounts concurrently

@celery_app.task
def crawl_from_transactions_task():
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    accounts_path = PROJECT_ROOT / "tracked-accounts.json"
    accounts = load_json(accounts_path)
    asyncio.run(crawl_all_from_accounts(accounts))


@celery_app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(
        timedelta(seconds=40),  # Run every 40 secs
        crawl_from_transactions_task.s(),
    )