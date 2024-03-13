from typing import Dict
from typing import List

from pydantic import BaseModel

from .data_models import AaveDebtData
from .data_models import AaveSupplyData
from .data_models import AssetDetailsData
from .data_models import liquidationData
from .data_models import RateDetailsData
from .data_models import volumeData


class EpochBaseSnapshot(BaseModel):
    begin: int
    end: int


class SnapshotBase(BaseModel):
    contract: str
    chainHeightRange: EpochBaseSnapshot
    timestamp: int


class AavePoolTotalAssetSnapshot(SnapshotBase):
    totalAToken: Dict[
        str,
        AaveSupplyData,
    ]  # block number to corresponding total supply
    liquidityRate: Dict[str, int]
    liquidityIndex: Dict[str, int]
    totalVariableDebt: Dict[str, AaveDebtData]
    totalStableDebt: Dict[str, AaveDebtData]
    variableBorrowRate: Dict[str, int]
    stableBorrowRate: Dict[str, int]
    variableBorrowIndex: Dict[str, int]
    lastUpdateTimestamp: Dict[str, int]
    isolationModeTotalDebt: Dict[str, int]
    assetDetails: Dict[str, AssetDetailsData]
    rateDetails: Dict[str, RateDetailsData]
    availableLiquidity: Dict[str, AaveSupplyData]


class AaveSupplyVolumeSnapshot(SnapshotBase):
    borrow: volumeData
    repay: volumeData
    supply: volumeData
    withdraw: volumeData
    liquidation: volumeData
    events: List[Dict]
    liquidationList: List[liquidationData]
