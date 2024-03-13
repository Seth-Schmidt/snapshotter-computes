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
from .utils.core import get_asset_supply_and_debt
from .utils.models.data_models import AssetTotalData
from .utils.models.message_models import AavePoolTotalAssetSnapshot
from .utils.models.message_models import EpochBaseSnapshot


class AssetTotalSupplyProcessor(GenericProcessor):
    transformation_lambdas = None

    def __init__(self) -> None:
        self.transformation_lambdas = []
        self._logger = logger.bind(module='AssetTotalSupplyProcessor')

    async def _compute_single(
        self,
        data_source_contract_address: str,
        min_chain_height: int,
        max_chain_height: int,
        rpc_helper: RpcHelper,

    ) -> Optional[Dict[str, Union[int, float]]]:

        epoch_asset_snapshot_map_total_supply = dict()
        epoch_asset_snapshot_map_liquidity_rate = dict()
        epoch_asset_snapshot_map_liquidity_index = dict()

        epoch_asset_snapshot_map_total_stable_debt = dict()
        epoch_asset_snapshot_map_total_variable_debt = dict()
        epoch_asset_snapshot_map_variable_borrow_rate = dict()
        epoch_asset_snapshot_map_stable_borrow_rate = dict()
        epoch_asset_snapshot_map_variable_borrow_index = dict()
        epoch_asset_snapshot_map_variable_borrow_rate = dict()

        epoch_asset_snapshot_map_last_update_timestamp = dict()
        epoch_asset_snapshot_map_isolated_debt = dict()
        epoch_asset_snapshot_map_asset_details = dict()
        epoch_asset_snapshot_map_available_liquidity = dict()
        epoch_asset_snapshot_map_rate_details = dict()

        max_block_timestamp = int(time.time())

        asset_supply_debt_total: Dict[str, AssetTotalData] = await get_asset_supply_and_debt(
            asset_address=data_source_contract_address,
            from_block=min_chain_height,
            to_block=max_chain_height,
            rpc_helper=rpc_helper,
        )

        for block_num in range(min_chain_height, max_chain_height + 1):
            block_asset_supply_debt = asset_supply_debt_total.get(block_num)
            fetch_ts = True if block_num == max_chain_height else False

            epoch_asset_snapshot_map_total_supply[
                f'block{block_num}'
            ] = block_asset_supply_debt.totalSupply
            epoch_asset_snapshot_map_liquidity_rate[
                f'block{block_num}'
            ] = block_asset_supply_debt.liquidityRate
            epoch_asset_snapshot_map_liquidity_index[
                f'block{block_num}'
            ] = block_asset_supply_debt.liquidityIndex
            epoch_asset_snapshot_map_total_stable_debt[
                f'block{block_num}'
            ] = block_asset_supply_debt.totalStableDebt
            epoch_asset_snapshot_map_total_variable_debt[
                f'block{block_num}'
            ] = block_asset_supply_debt.totalVariableDebt
            epoch_asset_snapshot_map_variable_borrow_rate[
                f'block{block_num}'
            ] = block_asset_supply_debt.variableBorrowRate
            epoch_asset_snapshot_map_stable_borrow_rate[
                f'block{block_num}'
            ] = block_asset_supply_debt.stableBorrowRate
            epoch_asset_snapshot_map_variable_borrow_index[
                f'block{block_num}'
            ] = block_asset_supply_debt.variableBorrowIndex
            epoch_asset_snapshot_map_last_update_timestamp[
                f'block{block_num}'
            ] = block_asset_supply_debt.lastUpdateTimestamp
            epoch_asset_snapshot_map_asset_details[
                f'block{block_num}'
            ] = block_asset_supply_debt.assetDetails
            epoch_asset_snapshot_map_available_liquidity[
                f'block{block_num}'
            ] = block_asset_supply_debt.availableLiquidity
            epoch_asset_snapshot_map_rate_details[
                f'block{block_num}'
            ] = block_asset_supply_debt.rateDetails
            epoch_asset_snapshot_map_isolated_debt[
                f'block{block_num}'
            ] = block_asset_supply_debt.isolationModeTotalDebt

            if fetch_ts:
                if not block_asset_supply_debt.timestamp:
                    self._logger.error(
                        (
                            'Could not fetch timestamp against max block'
                            ' height in epoch {} - {}to calculate pair'
                            ' reserves for contract {}. Using current time'
                            ' stamp for snapshot construction'
                        ),
                        data_source_contract_address,
                        min_chain_height,
                        max_chain_height,
                    )
                else:
                    max_block_timestamp = block_asset_supply_debt.timestamp
        asset_total_snapshot = AavePoolTotalAssetSnapshot(
            **{
                'totalAToken': epoch_asset_snapshot_map_total_supply,
                'liquidityRate': epoch_asset_snapshot_map_liquidity_rate,
                'liquidityIndex': epoch_asset_snapshot_map_liquidity_index,
                'totalVariableDebt': epoch_asset_snapshot_map_total_variable_debt,
                'totalStableDebt': epoch_asset_snapshot_map_total_stable_debt,
                'variableBorrowRate': epoch_asset_snapshot_map_variable_borrow_rate,
                'stableBorrowRate': epoch_asset_snapshot_map_stable_borrow_rate,
                'variableBorrowIndex': epoch_asset_snapshot_map_variable_borrow_index,
                'lastUpdateTimestamp': epoch_asset_snapshot_map_last_update_timestamp,
                'isolationModeTotalDebt': epoch_asset_snapshot_map_isolated_debt,
                'assetDetails': epoch_asset_snapshot_map_asset_details,
                'rateDetails': epoch_asset_snapshot_map_rate_details,
                'availableLiquidity': epoch_asset_snapshot_map_available_liquidity,
                'chainHeightRange': EpochBaseSnapshot(
                    begin=min_chain_height, end=max_chain_height,
                ),
                'timestamp': max_block_timestamp,
                'contract': data_source_contract_address,
            },
        )

        return asset_total_snapshot
    
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

        self._logger.debug(f'asset supply {data_source_contract_address} computation init time {time.time()}')

        snapshot = await self._compute_single(
            data_source_contract_address=data_source_contract_address,
            min_chain_height=min_chain_height,
            max_chain_height=max_chain_height,
            rpc_helper=rpc_helper,
        )

        self._logger.debug(f'asset supply {data_source_contract_address} computation end time {time.time()}')

        return [(data_source_contract_address, snapshot)]