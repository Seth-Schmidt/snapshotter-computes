import json
from asyncio import gather

from snapshotter.utils.default_logger import logger
from snapshotter.utils.rpc import get_contract_abi_dict
from snapshotter.utils.rpc import RpcHelper
from web3 import Web3

from ..settings.config import settings as worker_settings
from .constants import aave_oracle_abi
from .constants import pool_contract_obj

pricing_logger = logger.bind(module='PowerLoom|Aave|Pricing')


async def get_all_asset_prices(
    from_block,
    to_block,
    rpc_helper: RpcHelper,
    debug_log=False,
):
    try:

        # fetch all assets list from the pool contract
        # https://github.com/aave/aave-v3-core/blob/master/contracts/protocol/pool/Pool.sol#L516
        [asset_list] = await rpc_helper.web3_call(
            tasks=[pool_contract_obj.functions.getReservesList()]
        )

        abi_dict = get_contract_abi_dict(
            abi=aave_oracle_abi,
        )

        # get all asset prices in the block range from the Aave Oracle contract
        # https://docs.aave.com/developers/core-contracts/aaveoracle
        asset_prices_bulk = await rpc_helper.batch_eth_call_on_block_range(
            abi_dict=abi_dict,
            contract_address=worker_settings.contract_addresses.aave_oracle,
            from_block=from_block,
            to_block=to_block,
            function_name='getAssetsPrices',
            params=[asset_list],
        )

        if debug_log:
            pricing_logger.debug(
                f'Retrieved bulk prices for aave assets: {asset_prices_bulk}',
            )

        all_assets_price_dict = {block_num: {} for block_num in range(from_block, to_block + 1)}

        # match each asset to its price for each block, prices are returned in the given order
        for i, block_num in enumerate(range(from_block, to_block + 1)):
            matches = zip(asset_list, asset_prices_bulk[i][0])

            for match in matches:
                # all asset prices are returned with 8 decimal format
                all_assets_price_dict[block_num][match[0]] = match[1]

        return all_assets_price_dict

    except Exception as err:
        pricing_logger.opt(exception=True, lazy=True).trace(
            (
                'Error while calculating bulk asset prices:'
                ' err: {err}'
            ),
            err=lambda: str(err),
        )
        raise err
