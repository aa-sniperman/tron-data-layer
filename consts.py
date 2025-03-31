from pydantic import BaseModel

ROUTER_ADDRESS = "TXF1xDbVGdxFGbovmmmXvBGu8ZiE3Lq4mR"
SUNPUMP_ADDRESS = "TTfvyrAz86hbZk5iDpKD78pqLGgi8C7AAw"

class TokenConfig(BaseModel):
    address: str
    pair: str
    symbol: str

token_configs = {
    "sunana": TokenConfig(
        address="TXme8qsGdorboWFTX3E2XBWkmLNq4h7Kbx",
        pair="TJ9g2SzMSH7yV71AtpEjLa4HQyRkSGYHW4",
        symbol="SUNANA"
    )
}