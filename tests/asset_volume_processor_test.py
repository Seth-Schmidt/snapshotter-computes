import asyncio

from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.rpc import RpcHelper

from ..pool_supply_volume import AssetSupplyVolumeProcessor
from ..utils.models.message_models import AaveSupplyVolumeSnapshot


async def test_asset_volume_processor():
    # Mock your parameters
    from_block = 19422690
    to_block = from_block + 9
    process_unit = SnapshotProcessMessage(
        begin=from_block,
        end=to_block,
        epochId=0,
        day=1,
    )

    processor = AssetSupplyVolumeProcessor()
    rpc_helper = RpcHelper()

    [(data_source_contract_address, asset_volume_snapshot)] = await processor.compute(
        msg_obj=process_unit,
        rpc_helper=rpc_helper,
        anchor_rpc_helper=None,
        ipfs_reader=None,
        protocol_state_contract=None,
    )

    assert isinstance(asset_volume_snapshot, AaveSupplyVolumeSnapshot)

    print('PASSED')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_asset_volume_processor())
