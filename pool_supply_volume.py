import time
from typing import Dict
from typing import Optional
from typing import Union

from ipfs_client.main import AsyncIPFSClient
from snapshotter.settings.config import settings
from snapshotter.utils.callback_helpers import GenericProcessor
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.rpc import RpcHelper

from .settings.config import settings as module_settings
from .utils.core import get_asset_trade_volume
from .utils.models.message_models import AaveSupplyVolumeSnapshot
from .utils.models.message_models import EpochBaseSnapshot


class AssetSupplyVolumeProcessor(GenericProcessor):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='AssetSupplyVolumeProcessor')

    async def _compute_single(
        self,
        data_source_contract_address: str,
        min_chain_height: int,
        max_chain_height: int,
        rpc_helper: RpcHelper,

    ) -> Optional[Dict[str, Union[int, float]]]:

        max_block_timestamp = int(time.time())

        result = await get_asset_trade_volume(
            asset_address=data_source_contract_address,
            from_block=min_chain_height,
            to_block=max_chain_height,
            rpc_helper=rpc_helper,
        )

        if result.get('timestamp', 0) > 0:
            max_block_timestamp = result['timestamp']

        result.pop('timestamp', None)

        # flatten the event logs into a single list
        events = [log for key in result.keys() for log in result[key]['logs']]

        supply_volume_snapshot = AaveSupplyVolumeSnapshot(
            contract=data_source_contract_address,
            chainHeightRange=EpochBaseSnapshot(
                begin=min_chain_height, 
                end=max_chain_height,
            ),
            timestamp=max_block_timestamp,
            borrow=result['borrow']['totals'],
            repay=result['repay']['totals'],
            supply=result['supply']['totals'],
            withdraw=result['withdraw']['totals'],
            liquidation=result['liquidation']['totalLiquidatedCollateral'],
            events=events,
            liquidationList=result['liquidation']['liquidations'],
        )

        return supply_volume_snapshot
    
    def _gen_pair_idx_to_compute(self, msg_obj: SnapshotProcessMessage):
        monitored_pairs = module_settings.initial_assets
        current_epoch = msg_obj.epochId
        snapshotter_hash = hash(int(settings.instance_id.lower(), 16))
        current_day = msg_obj.day
        return (current_epoch + snapshotter_hash + settings.slot_id + current_day) % len(monitored_pairs)

    async def compute(
        self,
        msg_obj: SnapshotProcessMessage,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
    ):
        min_chain_height = msg_obj.begin
        max_chain_height = msg_obj.end

        monitored_pairs = module_settings.initial_assets
        pair_idx = self._gen_pair_idx_to_compute(msg_obj)
        data_source_contract_address = monitored_pairs[pair_idx]

        self._logger.debug(f'supply volume {data_source_contract_address} computation init time {time.time()}')

        snapshot = await self._compute_single(
            data_source_contract_address=data_source_contract_address,
            min_chain_height=min_chain_height,
            max_chain_height=max_chain_height,
            rpc_helper=rpc_helper,
        )

        self._logger.debug(f'supply volume {data_source_contract_address} computation end time {time.time()}')

        return [(data_source_contract_address, snapshot)]