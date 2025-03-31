from enum import Enum

class NormalTransactionType(str, Enum):
    TRIGGER_SMART_CONTRACT = "TriggerSmartContract"
    UNDELEGATE_RESOURCE_CONTRACT = "UnDelegateResourceContract"
    TRANSFER_CONTRACT = "TransferContract"
    INTERNAL = "Internal"
    TRANSFER_ASSET_CONTRACT = "TransferAssetContract"
