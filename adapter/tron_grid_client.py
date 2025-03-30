import httpx
from typing import Any, Dict, List, Optional
from settings import settings

BASE_URL = "https://api.trongrid.io"  # Replace with your RPC endpoint if needed

class TronGridClient:
    def __init__(self):
        self.client = httpx.AsyncClient(base_url=BASE_URL, timeout=10.0, headers= {
        'accept': 'application/json',
        'TRON-PRO-API-KEY': settings.keys.trongrid_api_key,
      })

    async def get_trx_balance(self, address: str) -> Optional[int]:
        res = await self.client.post("/walletsolidity/getaccount", json={"address": address, "visible": True})
        return res.json().get("balance")

    async def broadcast_tx(self, signed_tx: Dict[str, Any]) -> Dict[str, Any]:
        res = await self.client.post("/wallet/broadcasttransaction", json=signed_tx)
        return {**res.json(), "transaction": signed_tx}

    async def account_info(self, address: str) -> List[Dict[str, Any]]:
        res = await self.client.get(f"/v1/accounts/{address}")
        return res.json().get("data", [])

    async def account_transactions(self, address: str) -> Dict[str, Any]:
        res = await self.client.get(f"/v1/accounts/{address}/transactions")
        return res.json()

    async def contract_events(self, address: str, event_name: str, min_block_ts: int) -> List[Dict[str, Any]]:
        path = f"/v1/contracts/{address}/events?event_name={event_name}&order_by=block_timestamp%2Cdesc&min_block_timestamp={min_block_ts}&limit=200"
        return await self.recursive_get([], path)

    async def get_trc10(self, address: str) -> Dict[str, Any]:
        res = await self.client.post("/wallet/getassetissuebyaccount", json={"address": address, "visible": True})
        return res.json()

    async def get_link(self, link: str) -> httpx.Response:
        return await self.client.get(link)

    async def recursive_get(self, current: List[Dict[str, Any]], link: str) -> List[Dict[str, Any]]:
        res = await self.get_link(link)
        data = res.json()
        next_data = current + data.get("data", [])

        next_link = data.get("meta", {}).get("links", {}).get("next")
        if next_link:
            return await self.recursive_get(next_data, next_link[len(BASE_URL):])  # Remove RPC base URL
        return next_data

    async def get_list_exchanges(self) -> Dict[str, Any]:
        res = await self.client.get("/walletsolidity/listexchanges?visible=true")
        return res.json()

    async def get_tx_info(self, tx_id: str) -> Dict[str, Any]:
        res = await self.client.post("/walletsolidity/gettransactionbyid", json={"value": tx_id})
        return res.json()

    async def get_pending_tx(self, tx_id: str) -> Dict[str, Any]:
        res = await self.client.post("/wallet/gettransactionfrompending", json={"value": tx_id})
        return res.json()

    async def get_from_txs(self, account: str, min_ts: int) -> List[Dict[str, Any]]:
        path = f"/v1/accounts/{account}/transactions?only_from=true&min_timestamp={min_ts}&limit=200&order_by=block_timestamp,asc"
        res = await self.client.get(path)
        return res.json().get("data", [])
    
    async def get_to_txs(self, account: str, min_ts: int) -> List[Dict[str, Any]]:
        path = f"/v1/accounts/{account}/transactions?only_to=true&min_timestamp={min_ts}&limit=200&order_by=block_timestamp,asc"
        res = await self.client.get(path)
        return res.json().get("data", [])
    
    async def get_trc20_txs(self, account: str, min_ts: int) -> List[Dict[str, Any]]:
        path = f"/v1/accounts/{account}/transactions/trc20?min_timestamp={min_ts}&limit=200&order_by=block_timestamp,asc"
        res = await self.client.get(path)
        return res.json().get("data", [])
    
    async def estimate_energy(self, data: Dict[str, Any]) -> Dict[str, Any]:
        path = "/wallet/estimateenergy"
        res = await self.client.post(path, json={**data, "visible": True})
        return res.json()

    async def get_events_by_tx_id(self, tx_id: str) -> List[Dict[str, Any]]:
        path = f"/v1/transactions/{tx_id}/events"
        res = await self.client.get(path)
        return res.json().get("data", [])

# Initialize a client instance
tron_grid_client = TronGridClient()