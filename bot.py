import sys
import asyncio
import json
import os
from datetime import datetime
from types import SimpleNamespace
from functools import partial
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    JobQueue,
    Job,
    filters,
)
from api import get_user_by_phone, get_orders_by_user_id
from auth import extract_phone_number, save_user
from db import init_db, get_all_users
from config import BOT_TOKEN, OFFICE_ALIASES
from logs_setup import setup_logging

# =========================
# ЛОГИРОВАНИЕ
# =========================
logger = setup_logging("logs/bot.log")

# =========================
# КЭШ ДЛЯ ВАЧДОГА (файл + память)
# =========================
CACHE_FILE = "status_cache.json"
_status_cache: dict[str, str] = {}  # {order_number: formatted_text}


def load_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except Exception as e:
            logger.warning(f"Не удалось прочитать {CACHE_FILE}: {e}")
    return {}


def save_cache():
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(_status_cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"Не удалось сохранить {CACHE_FILE}: {e}")


# =========================
# ХЕЛПЕРЫ
# =========================
def emoji_for_status_line(status: str) -> str:
    s = (status or "").lower()
    if "готово" in s:
        return "✅"
    if "в пути" in s:
        return "🚚"
    if "к заказу" in s:
        return "🕐"
    if "отказ" in s:
        return "❌"
    return "📦"


def format_order_status(order: dict) -> str:
    """Формирует текст статусов по позициям + адрес офиса."""
    number = order.get("number", "-")
    delivery_office = order.get("deliveryOffice", "") or ""
    office_address = OFFICE_ALIASES.get(delivery_office, delivery_office)

    lines = [f"📦 Заказ №{number}", f"🏢 Офис: {office_address}\n"]
    for pos in order.get("positions", []):
        brand = (pos.get("brand") or "").strip()
        desc = (pos.get("description") or "").strip()
        status = pos.get("status") or ""
        price = pos.get("priceOut", "")
        qty = pos.get("quantity", "1")

        lines.append(
            f"{emoji_for_status_line(status)} {brand} {desc}\n"
            f"   💵 {price} ₽ × {qty}\n"
            f"   📄 {status}"
        )
    return "\n".join(lines)


# =========================
# ХЭНДЛЕРЫ БОТА (без inline-кнопок — всё просто и надёжно)
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Приветствие + запрос номера телефона."""
    button = KeyboardButton("Отправить номер телефона", request_contact=True)
    kb = ReplyKeyboardMarkup([[button]], one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text(
        "👋 Привет! Отправь свой номер телефона, чтобы получить информацию о заказах.",
        reply_markup=kb,
    )


async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Авторизация и моментальный вывод заказов (с позициями построчно)."""
    try:
        # Получаем телефон
        if update.message.contact:
            phone = update.message.contact.phone_number
        else:
            phone = extract_phone_number(update.message.text or "")

        if not phone:
            await update.message.reply_text("❌ Не удалось распознать номер телефона.")
            return

        # Нормализуем
        phone = (
            phone.replace("+", "")
            .replace(" ", "")
            .replace("-", "")
            .replace("(", "")
            .replace(")", "")
            .strip()
        )
        if phone.startswith("8"):
            phone = "7" + phone[1:]
        elif not phone.startswith("7"):
            phone = "7" + phone

        logger.info(f"Авторизация телефона: {phone}")

        # Забираем пользователя из ABCP
        user = get_user_by_phone(phone)
        if not user:
            await update.message.reply_text("❌ Пользователь не найден.")
            return

        user_id = user.get("userId")
        name = user.get("name", "—")
        balance = user.get("balance", "0.00")
        debt = user.get("debt", "0.00")

        # Сохраняем в БД
        save_user(update.effective_user.id, phone, user_id)
        logger.info(f"Пользователь {name} ({user_id}) успешно авторизован.")

        await update.message.reply_text(
            f"✅ Авторизация прошла успешно!\n"
            f"👤 {name}\n"
            f"💰 Баланс: {balance} ₽\n"
            f"💸 Задолженность: {debt} ₽\n\n"
            f"⏳ Загружаем список заказов..."
        )

        # Грузим заказы
        orders = get_orders_by_user_id(user_id)
        logger.info(f"Получено заказов: {len(orders)}")

        if not orders:
            await update.message.reply_text("🕐 У вас пока нет заказов.")
            return

        # Выводим каждый заказ: шапка + позиции построчно
        for order in orders:
            office = OFFICE_ALIASES.get(order.get("deliveryOffice", ""), "—")
            header = (
                f"📦 Заказ №{order.get('number', '-')}\n"
                f"📅 {order.get('date', '-')}\n"
                f"🏬 {office}\n"
                f"💰 Сумма: {order.get('sum', 0)} ₽\n"
                f"💳 Оплата: {order.get('paymentType', '-')}\n"
                f"📍 Статус: {'Оплачен' if order.get('paid') else 'Не оплачен'}\n\n"
                f"🧾 Позиции:"
            )
            await update.message.reply_text(header)

            for pos in order.get("positions", []):
                brand = pos.get("brand", "")
                desc = pos.get("description", "")
                status = pos.get("status", "")
                price = pos.get("priceOut", "")
                quantity = pos.get("quantity", "1")

                emoji = emoji_for_status_line(status)
                text = (
                    f"{emoji} {brand} {desc}\n"
                    f"   💵 {price} ₽ × {quantity}\n"
                    f"   📄 {status}\n"
                )
                await update.message.reply_text(text)

    except Exception as e:
        logger.exception(f"Ошибка при авторизации или выводе заказов: {e}")
        await update.message.reply_text("⚠️ Ошибка при загрузке заказов. Подробности в логах.")


# =========================
# ВАЧДОГ: периодическая проверка заказов (каждые 60 сек)
# =========================
def build_changes_messages_for_user(user: dict) -> list[tuple[int, str]]:
    """
    Возвращает список (chat_id, message) для отправки,
    только если у заказа изменился «сводный текст по позициям».
    """
    chat_id = user.get("telegram_id") or user.get("chat_id")
    user_id = user.get("user_id") or user.get("abcp_user_id")
    if not chat_id or not user_id:
        logger.warning(f"⛔ Пропущен пользователь (нет chat_id или user_id): {user}")
        return []

    orders = get_orders_by_user_id(user_id)
    messages: list[tuple[int, str]] = []

    for order in orders:
        num = order.get("number")
        text = format_order_status(order)
        prev = _status_cache.get(num)

        if text != prev:
            # Изменилась любая позиция/статус — отправим
            messages.append((chat_id, f"📢 Обновление статусов:\n\n{text}"))
            _status_cache[num] = text

    return messages


async def watchdog_job(context: ContextTypes.DEFAULT_TYPE):
    """Запускается JobQueue-ой каждые 60 сек внутри event-loop приложения."""
    try:
        users = get_all_users()
        if not users:
            logger.info("[WD] Пользователей нет.")
            return

        total_msgs = 0
        for u in users:
            msgs = await asyncio.to_thread(build_changes_messages_for_user, u)
            for chat_id, text in msgs:
                try:
                    await context.bot.send_message(chat_id=chat_id, text=text)
                    total_msgs += 1
                except Exception as e:
                    logger.exception(f"[WD] Ошибка отправки сообщения пользователю {chat_id}: {e}")

        if total_msgs:
            save_cache()
            logger.info(f"[WD] Отправлено уведомлений: {total_msgs}")
        else:
            logger.info("[WD] Изменений нет.")

    except Exception as e:
        logger.exception(f"[WD] Ошибка фоновой проверки: {e}")



async def _manual_watchdog_loop(
    app: Application, interval: int, first: int
) -> None:
    """Ручной запуск вачдога, если JobQueue недоступен."""
    try:
        if first:
            await asyncio.sleep(first)

        context = SimpleNamespace(bot=app.bot, application=app)
        while True:
            await watchdog_job(context)  # type: ignore[arg-type]
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("[WD] Ручной вачдог остановлен.")
        raise


async def _start_manual_watchdog(app: Application, interval: int, first: int) -> None:
    logger.warning(
        "JobQueue недоступна. Фоновая проверка заказов будет выполняться вручную. "
        "Установите python-telegram-bot[job-queue], чтобы вернуть стандартную работу."
    )
    app.create_task(_manual_watchdog_loop(app, interval=interval, first=first))

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    # Windows: корректная политика event-loop
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # БД + кеш
    init_db()
    _status_cache = load_cache()

    # Telegram-приложение
    app = Application.builder().token(BOT_TOKEN).build()

    # Хэндлеры
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_contact))

    # Запускаем вачдог каждые 60 сек (первый запуск через 10 сек)
    watchdog_interval = 60
    watchdog_first = 10

    job_queue = getattr(app, "job_queue", None)

    if job_queue is not None:
        job_queue.run_repeating(
            watchdog_job,
            interval=watchdog_interval,
            first=watchdog_first,
            name="orders_watchdog",
        )
    else:
        app.post_init(
            partial(
                _start_manual_watchdog,
                interval=watchdog_interval,
                first=watchdog_first,
            )
        )

    logger.info("🤖 Бот запущен и готов к работе (watchdog: 60 сек).")
    app.run_polling()
