from lighter import _remove_all_temp_arch_files
from os import name as os_name
from random import randint
from loguru import logger
from time import sleep
import asyncio

from modules.retry import DataBaseError
from modules.utils import choose_mode, async_sleep
from modules import *
import settings


def initialize_account(module_data: dict, group_data: dict = None):
    lighter_account = LighterAccount(
        privatekey=module_data["privatekey"],
        encoded_privatekey=module_data["encoded_privatekey"],
        address=module_data["address"],
        label=module_data["label"],
        proxy=module_data["proxy"],
        db=db,
        group_data=group_data,
    )
    lighter_account.log_message("Started")

    return lighter_account


async def run_modules(mode: int, module_data: dict, sem: asyncio.Semaphore):
    async with address_locks[module_data["address"]]:
        async with sem:
            lighter_account = None
            try:
                lighter_account = initialize_account(module_data)
                module_data["module_info"]["status"] = await lighter_account.run_mode(mode=mode)

            except Exception as err:
                if lighter_account:
                    logger.error(f'[-] {lighter_account.label} | Account Error: {err}')
                    await db.append_report(key=lighter_account.encoded_privatekey, text=str(err), success=False)
                else:
                    logger.error(f'[-] {module_data["label"]} | Global error: {err}')

            finally:
                if lighter_account:
                    await lighter_account.close_sessions()
                    await lighter_account.remove_arch_file()

                if type(module_data) == dict:
                    if mode == 1:
                        last_module = await db.remove_module(module_data=module_data)
                    else:
                        last_module = await db.remove_account(module_data=module_data)

                    reports = await db.get_account_reports(
                        key=lighter_account.encoded_privatekey,
                        label=lighter_account.label,
                        address=lighter_account.address,
                        last_module=last_module,
                        mode=mode,
                    )
                    await TgReport().send_log(logs=reports)

                    if module_data["module_info"]["status"] is True:
                        await async_sleep(randint(*settings.SLEEP_AFTER_ACC))
                    else:
                        await async_sleep(10)


async def run_pair(mode: int, group_data: dict, sem: asyncio.Semaphore):
    async with MultiLock([wallet_data["address"] for wallet_data in group_data["wallets_data"]]):
        async with sem:
            lighter_accounts = []
            try:
                lighter_accounts = [
                    initialize_account(wallet_data, group_data=group_data)
                    for wallet_data in group_data["wallets_data"]
                ]
                group_data["module_info"]["status"] = await PairAccounts(
                    accounts=lighter_accounts,
                    group_data=group_data
                ).run()

            except Exception as err:
                logger.error(f'[-] Group {group_data["group_number"]} | Global error | {err}')
                await db.append_report(key=group_data["group_index"], text=str(err), success=False)

            finally:
                for lighter_account in lighter_accounts:
                    await lighter_account.close_sessions()
                    # await lighter_account.remove_arch_file()  # delete files after all groups in main

                await db.remove_group(group_data=group_data)

                reports = await db.get_account_reports(
                    key=group_data["group_index"],
                    label=f"Group {group_data['group_number']}",
                    address=None,
                    last_module=False,
                    mode=mode,
                )
                await TgReport().send_log(logs=reports)

                if group_data["module_info"]["status"] is True:
                    to_sleep = randint(*settings.SLEEP_AFTER_ACC)
                    logger.opt(colors=True).debug(f'[•] <white>Group {group_data["group_number"]}</white> | Sleep {to_sleep}s')
                    await async_sleep(to_sleep)
                else:
                    await async_sleep(10)


async def runner(mode: int):
    sem = asyncio.Semaphore(settings.THREADS)

    if mode in [4]:
        all_groups = db.get_all_groups()
        if all_groups != 'No more accounts left':
            await asyncio.gather(*[
                run_pair(group_data=group_data, mode=mode, sem=sem)
                for group_data in all_groups
            ])

    else:
        all_modules = db.get_all_modules(unique_wallets=mode in [2, 3])
        if all_modules != 'No more accounts left':
            await asyncio.gather(*[
                run_modules(module_data=module_data, mode=mode, sem=sem)
                for module_data in all_modules
            ])

    logger.success(f'All accounts done.')
    _remove_all_temp_arch_files()
    return 'Ended'


if __name__ == '__main__':
    if os_name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        db = DataBase()

        while True:
            mode = choose_mode()

            match mode.type:
                case "database":
                    db.create_modules(mode=mode.soft_id)

                case "module":
                    if asyncio.run(runner(mode=mode.soft_id)) == 'Ended': break
                    print('')

        sleep(0.1)
        input('\n > Exit\n')

    except DataBaseError as e:
        logger.error(f'[-] Database | {e}')

    except KeyboardInterrupt:
        pass

    finally:
        logger.info('[•] Soft | Closed')
