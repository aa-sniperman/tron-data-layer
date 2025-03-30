import base58

class TronUtils:
    @staticmethod
    def from_hex_address(address: str):
        return base58.b58encode_check(bytes.fromhex(address)).decode("utf-8")

    @staticmethod
    def to_hex_address(address: str):
        return base58.b58decode_check(address).hex().lower()
