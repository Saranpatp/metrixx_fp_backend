from telegram import Bot
from dotenv import load_dotenv
import os
import asyncio

load_dotenv()

async def send_async_message(message):
    token = os.getenv('BOT_TOKEN')
    chat_id = '-4268783657'
    bot = Bot(token=token)
    await bot.send_message(chat_id=chat_id, text=message)

def sent_msg(message):
    asyncio.run(send_async_message(message))

