import time
import json
import requests
import asyncio
from telegram import Bot
from config import BOT_TOKEN, API_URL_ORDERS, USERLOGIN, USERPSW, OFFICE_ALIASES
from db import get_all_users
from logs_setup import setup_logging

# Настройка логов
logger = setup_logging("logs/watchdog.log")
bot = Bot(token=BOT_TOKEN)
CACHE_FILE = "status_cache.json"

# Создаём единый event loop
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)


def load_cache():
    """Загрузка кэша предыдущих статусов заказов"""
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_cache(data):
    """Сохранение кэша"""
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_orders(user_id):
    """Получаем заказы пользователя"""
    params = {
        "userlogin": USERLOGIN,
        "userpsw": USERPSW,
        "userId": user_id
    }
    try:
        resp = requests.get(API_URL_ORDERS, params=params, timeout=15)
        if resp.status_code == 200:
            return resp.json()
        else:
            logger.error(f"Ошибка API ({resp.status_code}): {resp.text}")
    except Exception as e:
        logger.exception(f"Ошибка при запросе заказов: {e}")
    return []


def format_order_status(order):
    """Формирует сообщение по заказу с позициями и офисом выдачи"""
    number = order.get("number", "-")
    delivery_office = order.get("deliveryOffice", "")
    office_address = OFFICE_ALIASES.get(delivery_office, delivery_office)

    lines = [f"📦 Заказ №{number}", f"🏢 Офис: {office_address}\n"]

    for pos in order.get("positions", []):
        brand = pos.get("brand", "")
        desc = pos.get("description", "")
        status = pos.get("status", "")
        price = pos.get("priceOut", "")
        qty = pos.get("quantity", "1")

        # Определяем эмодзи по статусу
        if "готово" in status.lower():
            emoji = "✅"
        elif "в пути" in status.lower():
            emoji = "🚚"
        elif "к заказу" in status.lower():
            emoji = "🕐"
        elif "отказ" in status.lower():
            emoji = "❌"
        else:
            emoji = "📦"

        lines.append(
            f"{emoji} {brand} {desc}\n"
            f"   💵 {price} ₽ × {qty}\n"
            f"   📄 {status}"
        )

    return "\n".join(lines)


async def send_message_async(chat_id, text):
    """Асинхронная отправка сообщений"""
    try:
        await bot.send_message(chat_id, text)
        logger.info(f"📬 Сообщение отправлено пользователю {chat_id}")
    except Exception as e:
        logger.exception(f"Ошибка при отправке Telegram-сообщения: {e}")


def run_watchdog(interval=120):
    """Основной цикл мониторинга"""
    logger.info("🔍 Watchdog запущен.")
    cache = load_cache()

    while True:
        try:
            users = get_all_users()
            for user in users:
                chat_id = user.get("telegram_id") or user.get("chat_id")
                user_id = user.get("user_id") or user.get("abcp_user_id")

                if not chat_id or not user_id:
                    logger.warning(f"⛔ Пропущен пользователь (нет chat_id или user_id): {user}")
                    continue

                logger.info(f"Проверка заказов пользователя {user_id}")
                orders = get_orders(user_id)

                for order in orders:
                    number = order.get("number")
                    status_text = format_order_status(order)
                    prev_text = cache.get(number)

                    if status_text != prev_text:
                        message = f"📢 Обновление статусов:\n\n{status_text}"
                        loop.run_until_complete(send_message_async(chat_id, message))
                        cache[number] = status_text
                        logger.info(f"Изменение статусов в заказе {number}")

            save_cache(cache)

        except Exception as e:
            logger.exception(f"Ошибка в watchdog: {e}")

        time.sleep(interval)


if __name__ == "__main__":
    try:
        run_watchdog()
    finally:
        loop.close()