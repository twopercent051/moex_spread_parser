import asyncio

from tgbot.handlers.echo import router as echo_router
from tgbot.handlers.admin.main_block import router as admin_main_block
from tgbot.handlers.user.main_block import router as user_main_block
from tgbot.misc.scheduler import scheduler_jobs
from tgbot.models.redis_connector import RedisConnector as rds

from create_bot import bot, dp, scheduler, logger, register_global_middlewares, config


admin_router = [
    admin_main_block,
]


user_router = [
    user_main_block,
]


async def main():
    logger.info("Starting bot")
    dp.include_routers(
        *admin_router,
        *user_router,
        echo_router
    )

    try:
        register_global_middlewares(dp, config)
        await dp.start_polling(bot)
    finally:
        await dp.storage.close()
        await bot.session.close()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.error("Bot stopped!")
