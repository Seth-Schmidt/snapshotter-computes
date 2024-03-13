import asyncio

from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.rpc import RpcHelper

from ..pool_total_supply import AssetTotalSupplyProcessor
from ..utils.models.message_models import AavePoolTotalAssetSnapshot


async def test_total_supply_processor():
    # Mock your parameters
    from_block = 19422690
    to_block = from_block + 9
    process_unit = SnapshotProcessMessage(
        begin=from_block,
        end=to_block,
        epochId=0,
        day=1,
    )

    processor = AssetTotalSupplyProcessor()
    rpc_helper = RpcHelper()

    [(data_source_contract_address, asset_total_snapshot)] = await processor.compute(
        msg_obj=process_unit,
        rpc_helper=rpc_helper,
        anchor_rpc_helper=None,
        ipfs_reader=None,
        protocol_state_contract=None,
    )

    assert isinstance(asset_total_snapshot, AavePoolTotalAssetSnapshot)
    assert len(asset_total_snapshot.totalAToken) == (to_block - from_block + 1), 'Should return data for all blocks'
    assert len(asset_total_snapshot.liquidityIndex) == (to_block - from_block + 1), 'Should return data for all blocks'

    print('PASSED')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_total_supply_processor())
