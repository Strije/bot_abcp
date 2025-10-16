import sys
import asyncio
import json
import os
from datetime import datetime
from telegram import (
    Update,
    KeyboardButton,
    ReplyKeyboardMarkup,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    JobQueue,
    Job,
    CallbackQueryHandler,
    filters,
)
from telegram.error import BadRequest
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
    office_address = OFFICE_ALIASES.get(delivery_office, delivery_office) or "—"
    date = order.get("date", "-")
    total = order.get("sum", "0")
    payment_type = order.get("paymentType", "—")
    paid = bool(order.get("paid"))
    comment = (order.get("comment") or "").strip()

    lines = [
        f"📦 Заказ №{number}",
        f"📅 Дата: {date}",
        f"🏢 Офис: {office_address}",
        f"💳 Способ оплаты: {payment_type}",
        f"💰 Сумма: {total} ₽",
        f"📍 Статус счёта: {'✅ Оплачен' if paid else '⏳ Не оплачен'}",
    ]

    if comment:
        lines.append(f"💬 Комментарий: {comment}")

    positions = order.get("positions", []) or []
    if positions:
        lines.append("")
        lines.append("🧾 Позиции:")
        for pos in positions:
            brand = (pos.get("brand") or "").strip()
            desc = (pos.get("description") or "").strip()
            status = pos.get("status") or ""
            price = pos.get("priceOut", "")
            qty = pos.get("quantity", "1")

            label = " ".join(filter(None, [brand, desc])) or "Позиция"
            lines.append(f"{emoji_for_status_line(status)} {label}")
            lines.append(f"   📄 {status}")
            lines.append(f"   📦 Кол-во: {qty} | 💵 {price} ₽")
            lines.append("")
        if lines and lines[-1] == "":
            lines.pop()
    else:
        lines.append("")
        lines.append("🧾 Позиции: нет данных")

    return "\n".join(lines)


def format_order_detail(order: dict) -> str:
    body = format_order_status(order)
    return f"{body}\n\nℹ️ Используйте кнопки ниже, чтобы вернуться к списку или обновить заказ."


def format_orders_overview(orders: list[dict]) -> str:
    if not orders:
        return "🕐 Активных заказов нет."

    lines = [
        f"📋 Найдено заказов: {len(orders)}",
        "Выберите заказ на клавиатуре ниже, чтобы посмотреть детали.",
        "",
    ]

    for idx, order in enumerate(orders, start=1):
        number = order.get("number", "-")
        date = order.get("date", "-")
        total = order.get("sum", "0")
        paid = bool(order.get("paid"))
        positions = order.get("positions", []) or []
        first_status = next((p.get("status") for p in positions if p.get("status")), "Статус неизвестен")
        lines.append(
            f"{idx}. №{number} • {date} • {emoji_for_status_line(first_status)} {first_status} • {total} ₽ • {'✅ Оплачен' if paid else '⏳ Не оплачен'}"
        )

    return "\n".join(lines)


def assign_order_tokens(
    orders: list[dict],
    existing_map: dict[str, str] | None = None,
) -> tuple[dict[str, str], dict[str, str]]:
    """Подбирает короткие токены для callback_data, чтобы избежать превышения лимита Telegram."""

    existing_map = existing_map or {}
    used_tokens: set[str] = set()
    number_to_token: dict[str, str] = {}
    token_to_number: dict[str, str] = {}
    counter = 0

    def next_token() -> str:
        nonlocal counter
        while True:
            candidate = format(counter, "x")  # короткая hex-запись
            counter += 1
            if candidate not in used_tokens:
                return candidate

    for order in orders:
        number = order.get("number")
        if not number:
            continue
        number_str = str(number)
        token = existing_map.get(number_str)
        if token and token not in used_tokens:
            assigned = token
        else:
            assigned = next_token()

        used_tokens.add(assigned)
        number_to_token[number_str] = assigned
        token_to_number[assigned] = number_str

    return number_to_token, token_to_number


def build_orders_keyboard(
    orders: list[dict],
    number_to_token: dict[str, str] | None = None,
) -> InlineKeyboardMarkup:
    if not orders:
        return InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Обновить", callback_data="orders:refresh")]])

    number_to_token = number_to_token or {}
    buttons: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []

    for order in orders:
        number = order.get("number")
        if not number:
            continue

        number_str = str(number)
        token = number_to_token.get(number_str)
        if not token:
            continue

        total = order.get("sum")
        title = f"№{number_str}"
        if total not in (None, ""):
            title += f" · {total} ₽"

        row.append(InlineKeyboardButton(title, callback_data=f"order:{token}"))
        if len(row) == 2:
            buttons.append(row)
            row = []

    if row:
        buttons.append(row)

    buttons.append([InlineKeyboardButton("🔄 Обновить список", callback_data="orders:refresh")])
    return InlineKeyboardMarkup(buttons)


def update_cache_from_orders(orders: list[dict]):
    changed = False
    for order in orders:
        number = order.get("number")
        if number is None:
            continue
        number_str = str(number)
        text = format_order_status(order)
        if _status_cache.get(number_str) != text:
            changed = True
        _status_cache[number_str] = text
    if changed:
        save_cache()


async def safe_edit_message_text(
    bot,
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> bool:
    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=reply_markup,
        )
        return True
    except BadRequest as exc:
        if "message is not modified" in str(exc).lower():
            if reply_markup is not None:
                try:
                    await bot.edit_message_reply_markup(
                        chat_id=chat_id,
                        message_id=message_id,
                        reply_markup=reply_markup,
                    )
                except BadRequest:
                    pass
            return True
        raise


async def safe_edit_query_message(
    query,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> bool:
    try:
        await query.edit_message_text(text=text, reply_markup=reply_markup)
        return True
    except BadRequest as exc:
        if "message is not modified" in str(exc).lower():
            if reply_markup is not None:
                try:
                    await query.edit_message_reply_markup(reply_markup=reply_markup)
                except BadRequest:
                    pass
            return True
        raise


# =========================
# ХЭНДЛЕРЫ БОТА
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
    """Авторизация и аккуратное отображение списка заказов."""
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

        context.user_data["abcp_user_id"] = user_id
        context.user_data["customer_name"] = name

        # Грузим заказы и сохраняем в user_data
        orders = await asyncio.to_thread(get_orders_by_user_id, user_id)
        logger.info(f"Получено заказов: {len(orders)}")

        chat_id = update.effective_chat.id
        context.user_data["active_chat_id"] = chat_id
        orders_list = orders or []
        number_to_token, token_to_number = assign_order_tokens(
            orders_list, context.user_data.get("orders_number_to_token")
        )
        context.user_data["orders_list"] = orders_list
        context.user_data["orders_map"] = {
            str(order.get("number")): order for order in orders_list if order.get("number")
        }
        context.user_data["orders_number_to_token"] = number_to_token
        context.user_data["orders_token_to_number"] = token_to_number

        if not orders_list:
            empty_text = "🕐 У вас пока нет заказов."
            keyboard = build_orders_keyboard([], number_to_token)
            prev_message_id = context.user_data.get("active_message_id")
            if prev_message_id:
                try:
                    await safe_edit_message_text(
                        context.bot,
                        chat_id,
                        prev_message_id,
                        empty_text,
                        reply_markup=keyboard,
                    )
                except Exception as edit_error:
                    logger.debug(f"Не удалось обновить прошлое сообщение: {edit_error}")
                    try:
                        await context.bot.delete_message(chat_id=chat_id, message_id=prev_message_id)
                    except Exception:
                        pass
                    msg = await update.message.reply_text(empty_text, reply_markup=keyboard)
                    context.user_data["active_message_id"] = msg.message_id
            else:
                msg = await update.message.reply_text(empty_text, reply_markup=keyboard)
                context.user_data["active_message_id"] = msg.message_id

            context.user_data["orders_overview_text"] = empty_text
            update_cache_from_orders([])
            return

        update_cache_from_orders(orders_list)

        overview_text = format_orders_overview(orders_list)
        refreshed_at = datetime.now().strftime("%d.%m.%Y %H:%M")
        overview_text = f"{overview_text}\n\n🔄 Обновлено: {refreshed_at}"
        keyboard = build_orders_keyboard(orders_list, number_to_token)

        prev_message_id = context.user_data.get("active_message_id")
        if prev_message_id:
            try:
                await safe_edit_message_text(
                    context.bot,
                    chat_id,
                    prev_message_id,
                    overview_text,
                    reply_markup=keyboard,
                )
            except Exception as edit_error:
                logger.debug(f"Не удалось обновить прошлое сообщение: {edit_error}")
                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=prev_message_id)
                except Exception:
                    pass
                msg = await update.message.reply_text(overview_text, reply_markup=keyboard)
                context.user_data["active_message_id"] = msg.message_id
        else:
            msg = await update.message.reply_text(overview_text, reply_markup=keyboard)
            context.user_data["active_message_id"] = msg.message_id

        context.user_data["orders_overview_text"] = overview_text
        context.user_data["orders_last_synced"] = refreshed_at
        context.user_data["view"] = "overview"

    except Exception as e:
        logger.exception(f"Ошибка при авторизации или выводе заказов: {e}")
        await update.message.reply_text("⚠️ Ошибка при загрузке заказов. Подробности в логах.")


async def handle_orders_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка inline-кнопок со списком заказов."""
    query = update.callback_query
    if not query:
        return

    data = (query.data or "").strip()
    chat_id = query.message.chat.id
    context.user_data["active_chat_id"] = chat_id
    context.user_data["active_message_id"] = query.message.message_id

    async def sync_orders() -> tuple[list[dict], str, InlineKeyboardMarkup] | None:
        user_id = context.user_data.get("abcp_user_id")
        if not user_id:
            await query.answer("Нет данных для обновления", show_alert=True)
            return None

        orders = await asyncio.to_thread(get_orders_by_user_id, user_id)
        orders_list = orders or []
        context.user_data["orders_list"] = orders_list
        context.user_data["orders_map"] = {
            str(order.get("number")): order
            for order in orders_list
            if order.get("number")
        }
        number_to_token, token_to_number = assign_order_tokens(
            orders_list, context.user_data.get("orders_number_to_token")
        )
        context.user_data["orders_number_to_token"] = number_to_token
        context.user_data["orders_token_to_number"] = token_to_number
        update_cache_from_orders(orders_list)
        refreshed_at = datetime.now().strftime("%d.%m.%Y %H:%M")
        context.user_data["orders_last_synced"] = refreshed_at
        overview_text = format_orders_overview(orders_list)
        overview_text = f"{overview_text}\n\n🔄 Обновлено: {refreshed_at}"
        context.user_data["orders_overview_text"] = overview_text
        keyboard = build_orders_keyboard(orders_list, number_to_token)
        return orders_list, overview_text, keyboard

    if data == "orders:back":
        orders = context.user_data.get("orders_list", [])
        number_to_token = context.user_data.get("orders_number_to_token", {})
        text_block = context.user_data.get("orders_overview_text")
        if text_block is None:
            synced = await sync_orders()
            if not synced:
                return
            orders, text_block, keyboard = synced
            number_to_token = context.user_data.get("orders_number_to_token", {})
        else:
            keyboard = build_orders_keyboard(orders, number_to_token)
        try:
            await safe_edit_query_message(query, text_block, keyboard)
        except Exception as edit_error:
            logger.debug(f"Не удалось показать список заказов: {edit_error}")
        await query.answer()
        context.user_data["view"] = "overview"
        return

    if data == "orders:refresh":
        synced = await sync_orders()
        if not synced:
            return
        _, text_block, keyboard = synced
        try:
            await safe_edit_query_message(query, text_block, keyboard)
        except Exception as edit_error:
            logger.debug(f"Не удалось обновить список заказов: {edit_error}")
        await query.answer("Список обновлён ✅", show_alert=False)
        context.user_data["view"] = "overview"
        return

    if data.startswith("order-refresh:"):
        token = data.split(":", 1)[1]
        token_to_number = context.user_data.get("orders_token_to_number", {})
        number = token_to_number.get(token)
        if not number:
            synced = await sync_orders()
            if not synced:
                return
            token_to_number = context.user_data.get("orders_token_to_number", {})
            number = token_to_number.get(token)
        if not number:
            await query.answer("Заказ не найден", show_alert=True)
            return
        synced = await sync_orders()
        if not synced:
            return
        order = context.user_data.get("orders_map", {}).get(number)
        if not order:
            keyboard = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("⬅️ Назад к списку", callback_data="orders:back")],
                    [InlineKeyboardButton("🔄 Обновить список", callback_data="orders:refresh")],
                ]
            )
            try:
                await safe_edit_query_message(
                    query,
                    "❌ Заказ не найден. Возможно, он был закрыт или удалён.",
                    keyboard,
                )
            except Exception as edit_error:
                logger.debug(f"Не удалось показать сообщение об отсутствии заказа: {edit_error}")
            await query.answer("Заказ не найден", show_alert=True)
            context.user_data["view"] = "overview"
            return

        number_to_token = context.user_data.get("orders_number_to_token", {})
        refreshed_token = number_to_token.get(str(order.get("number")))
        detail_text = format_order_detail(order)
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("⬅️ Назад к списку", callback_data="orders:back")],
                [
                    InlineKeyboardButton(
                        "🔄 Обновить заказ",
                        callback_data=f"order-refresh:{refreshed_token or token}",
                    )
                ],
            ]
        )
        try:
            await safe_edit_query_message(query, detail_text, keyboard)
        except Exception as edit_error:
            logger.debug(f"Не удалось обновить детали заказа: {edit_error}")
        await query.answer("Детали обновлены ✅", show_alert=False)
        context.user_data["view"] = f"order:{number}"
        return

    if data.startswith("order:"):
        token = data.split(":", 1)[1]
        token_to_number = context.user_data.get("orders_token_to_number", {})
        number = token_to_number.get(token)
        if not number:
            synced = await sync_orders()
            if not synced:
                return
            token_to_number = context.user_data.get("orders_token_to_number", {})
            number = token_to_number.get(token)
        if not number:
            keyboard = InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔄 Обновить список", callback_data="orders:refresh")]]
            )
            try:
                await safe_edit_query_message(
                    query,
                    "❌ Не удалось найти заказ. Обновите список и попробуйте снова.",
                    keyboard,
                )
            except Exception as edit_error:
                logger.debug(
                    f"Не удалось показать сообщение о неизвестном токене заказа: {edit_error}"
                )
            await query.answer("Заказ не найден", show_alert=True)
            context.user_data["view"] = "overview"
            return
        orders_map = context.user_data.get("orders_map", {})
        order = orders_map.get(number)
        if not order:
            synced = await sync_orders()
            if not synced:
                return
            order = context.user_data.get("orders_map", {}).get(number)

        if not order:
            keyboard = InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔄 Обновить список", callback_data="orders:refresh")]]
            )
            try:
                await safe_edit_query_message(
                    query,
                    "❌ Не удалось найти заказ. Обновите список и попробуйте снова.",
                    keyboard,
                )
            except Exception as edit_error:
                logger.debug(f"Не удалось показать сообщение об отсутствии заказа: {edit_error}")
            await query.answer("Заказ не найден", show_alert=True)
            context.user_data["view"] = "overview"
            return

        number_to_token = context.user_data.get("orders_number_to_token", {})
        refreshed_token = number_to_token.get(str(order.get("number")))
        detail_text = format_order_detail(order)
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("⬅️ Назад к списку", callback_data="orders:back")],
                [
                    InlineKeyboardButton(
                        "🔄 Обновить заказ",
                        callback_data=f"order-refresh:{refreshed_token or token}",
                    )
                ],
            ]
        )
        try:
            await safe_edit_query_message(query, detail_text, keyboard)
        except Exception as edit_error:
            logger.debug(f"Не удалось показать детали заказа: {edit_error}")
        await query.answer()
        context.user_data["view"] = f"order:{number}"
        return

    await query.answer()

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
        if num is None:
            continue
        num_str = str(num)
        text = format_order_status(order)
        prev = _status_cache.get(num_str)

        if text != prev:
            # Изменилась любая позиция/статус — отправим
            messages.append((chat_id, f"📢 Обновление статусов:\n\n{text}"))
            _status_cache[num_str] = text

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
    app.add_handler(CallbackQueryHandler(handle_orders_callback))
    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_contact))

    # Запускаем вачдог через JobQueue каждые 60 сек (первый запуск через 10 сек)
    app.job_queue.run_repeating(watchdog_job, interval=60, first=10, name="orders_watchdog")

    logger.info("🤖 Бот запущен и готов к работе (watchdog: 60 сек).")
    app.run_polling()
