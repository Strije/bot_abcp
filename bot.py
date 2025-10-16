import sys
import asyncio

from telegram import KeyboardButton, ReplyKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from api import get_orders_by_user_id, get_user_by_phone
from auth import extract_phone_number, save_user
from db import get_user_by_telegram_id, init_db
from config import BOT_TOKEN, OFFICE_ALIASES
from logs_setup import setup_logging

# =========================
# ЛОГИРОВАНИЕ
# =========================
logger = setup_logging("logs/bot.log")

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
    date = order.get("date", "-")
    delivery_office = order.get("deliveryOffice", "") or ""
    office_address = OFFICE_ALIASES.get(delivery_office, delivery_office)
    total_sum = order.get("sum", 0)
    payment_type = order.get("paymentType", "-")
    paid_text = "Оплачен" if order.get("paid") else "Не оплачен"

    lines = [
        f"📦 Заказ №{number}",
        f"📅 Дата: {date}",
        f"🏢 Офис: {office_address}",
        f"💳 Оплата: {payment_type}",
        f"💰 Сумма: {total_sum} ₽",
        f"📍 Статус оплаты: {paid_text}",
        "",
    ]
    for pos in order.get("positions", []):
        brand = (pos.get("brand") or "").strip()
        desc = (pos.get("description") or "").strip()
        status = pos.get("status") or ""
        price = pos.get("priceOut", "")
        qty = pos.get("quantity", "1")

        lines.extend(
            [
                f"{emoji_for_status_line(status)} {brand} {desc}",
                f"   💵 {price} ₽ × {qty}",
                f"   📄 {status}",
                "",
            ]
        )
    return "\n".join(lines).strip()


# =========================
# ПОМОЩНИКИ ДЛЯ ЗАГРУЗКИ И ОТПРАВКИ ЗАКАЗОВ
# =========================
async def fetch_orders(user_id: str) -> list[dict]:
    """Неблокирующая обёртка вокруг синхронного запроса заказов."""
    return await asyncio.to_thread(get_orders_by_user_id, user_id)


async def send_orders_to_chat(
    chat_id: int, user_id: str, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Возвращает количество найденных заказов и отправляет их пользователю."""

    orders = await fetch_orders(user_id)
    if not orders:
        await context.bot.send_message(chat_id=chat_id, text="🕐 У вас пока нет заказов.")
        return 0

    await context.bot.send_message(
        chat_id=chat_id, text=f"🧾 Найдено заказов: {len(orders)}"
    )
    for order in orders:
        await context.bot.send_message(chat_id=chat_id, text=format_order_status(order))

    return len(orders)


# =========================
# ХЭНДЛЕРЫ БОТА (без inline-кнопок — всё просто и надёжно)
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Приветствие + запрос номера телефона."""
    button = KeyboardButton("Отправить номер телефона", request_contact=True)
    kb = ReplyKeyboardMarkup([[button]], one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text(
        "👋 Привет! Отправь свой номер телефона кнопкой ниже, чтобы получить информацию о заказах.\n"
        "После авторизации можно в любой момент вызвать /orders и обновить статусы.",
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

        # Грузим заказы неблокирующим образом и отправляем пользователю
        total_orders = await send_orders_to_chat(update.effective_chat.id, user_id, context)
        logger.info(f"Отправлено заказов: {total_orders}")

    except Exception as e:
        logger.exception(f"Ошибка при авторизации или выводе заказов: {e}")
        await update.message.reply_text("⚠️ Ошибка при загрузке заказов. Подробности в логах.")


async def orders_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Позволяет авторизованному пользователю обновить список заказов."""

    message = update.effective_message
    user_data = get_user_by_telegram_id(update.effective_user.id)
    if not user_data:
        await message.reply_text(
            "⚠️ Я не знаю ваш номер телефона. Отправьте контакт через /start, чтобы авторизоваться."
        )
        return

    user_id = user_data.get("user_id")
    if not user_id:
        await message.reply_text("⚠️ Не удалось найти ID учётной записи. Отправьте контакт заново через /start.")
        return

    await message.reply_text("🔄 Обновляем статусы заказов...")
    total_orders = await send_orders_to_chat(
        update.effective_chat.id, user_id, context
    )
    logger.info(
        "Команда /orders выполнена: пользователь %s, заказов отправлено %s",
        update.effective_user.id,
        total_orders,
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отображает краткую справку по доступным действиям."""

    await update.effective_message.reply_text(
        "ℹ️ Доступные команды:\n"
        "• /start — авторизация по номеру телефона.\n"
        "• /orders — получить актуальные статусы заказов после авторизации.\n"
        "Также вы можете просто отправить свой номер телефоном или текстом, чтобы авторизоваться."
    )


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    # Windows: корректная политика event-loop
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # База данных
    init_db()

    # Telegram-приложение
    app = Application.builder().token(BOT_TOKEN).build()

    # Хэндлеры
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("orders", orders_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_contact))

    logger.info("🤖 Бот запущен и готов к работе.")
    app.run_polling()
