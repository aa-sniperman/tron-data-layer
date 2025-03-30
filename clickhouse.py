import clickhouse_connect
from settings import settings

async def get_async_ch_client():
    return await clickhouse_connect.get_async_client(
        host=settings.clickhouse.host,
        database=settings.clickhouse.database,
        username=settings.clickhouse.username,
        password=settings.clickhouse.password,
    )
