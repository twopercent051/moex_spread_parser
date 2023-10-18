from aiogram.types import Message
from aiogram.filters import Command
from aiogram import Router

from .filters import AdminFilter
from ...misc.parser import Parser

router = Router()
router.message.filter(AdminFilter())

parser = Parser()


@router.message(Command("start"))
async def admin_start_msg(message: Message):
    text = "Чтобы запустить парсер отправьте команду /restart"
    await message.answer(text)


@router.message(Command("restart"))
async def start_parser(message: Message):
    text = ["Выгруженные данные:", "-" * 5]
    parsed_items = await parser.parser()
    for item in parsed_items:
        row = f"{item['base_ticker']} / {item['future_ticker']}: <i>{item['quantity']}</i> свечей"
        text.append(row)
    await message.answer(text="\n".join(text))
