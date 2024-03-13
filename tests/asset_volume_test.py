import asyncio

from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from ..utils.constants import pool_data_provider_contract_obj
from ..utils.core import get_asset_trade_volume
from ..utils.models.data_models import DataProviderReserveData

# TODO: Test Liquidation event
async def test_total_supply_and_debt_calc():
    # Mock your parameters
    asset_address = Web3.to_checksum_address(
        '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
    )

    from_block = 19420920
    to_block = from_block + 9
    rpc_helper = RpcHelper()

    asset_volume_data = await get_asset_trade_volume(
        asset_address=asset_address,
        from_block=from_block,
        to_block=to_block,
        rpc_helper=rpc_helper,
    )

    from pprint import pprint
    pprint(asset_volume_data)

    print('PASSED')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_total_supply_and_debt_calc())
