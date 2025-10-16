import sys
import asyncio
import json
import os
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from telegram import (
    Update,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
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
from db import (
    init_db,
    get_all_users,
    get_order_status,
    get_order_message,
    update_order_status,
    get_user_order_snapshots,
    clear_order_message,
    get_user_id_by_order_number,
)
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


FILTER_MODES: dict[str, str] = {
    "all": "Все",
    "active": "В работе",
    "unpaid": "Неоплаченные",
}
DONE_KEYWORDS = ("готов", "выдан", "закрыт", "заверш", "отмен", "отказ")


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


def is_position_closed(status: str | None) -> bool:
    text = (status or "").lower()
    return any(keyword in text for keyword in DONE_KEYWORDS)


def is_order_active(order: dict) -> bool:
    positions = order.get("positions") or []
    if not positions:
        return not bool(order.get("paid"))
    return any(not is_position_closed(pos.get("status")) for pos in positions)


def is_order_unpaid(order: dict) -> bool:
    return not bool(order.get("paid"))


def calculate_orders_metrics(orders: list[dict]) -> dict[str, int]:
    total = len(orders)
    active = sum(1 for order in orders if is_order_active(order))
    unpaid = sum(1 for order in orders if is_order_unpaid(order))
    return {"total": total, "active": active, "unpaid": unpaid}


def filter_orders_for_view(orders: list[dict], mode: str) -> list[dict]:
    if mode == "active":
        return [order for order in orders if is_order_active(order)]
    if mode == "unpaid":
        return [order for order in orders if is_order_unpaid(order)]
    return list(orders)


def format_orders_overview(
    orders: list[dict],
    metrics: dict[str, int],
    filter_mode: str = "all",
    refreshed_at: str | None = None,
) -> str:
    total_count = metrics.get("total", len(orders))
    active_count = metrics.get("active", 0)
    unpaid_count = metrics.get("unpaid", 0)
    visible_count = len(orders)

    if total_count == 0:
        base_text = "🕐 Заказов пока нет."
        if refreshed_at:
            base_text = f"{base_text}\n\n🔄 Обновлено: {refreshed_at}"
        return base_text

    lines = [
        f"📋 Показано: {visible_count} из {total_count}",
        f"⚙️ Фильтр: {FILTER_MODES.get(filter_mode, FILTER_MODES['all'])}",
        f"🚧 В работе: {active_count} • 💸 Неоплаченных: {unpaid_count}",
        "",
    ]

    if not orders:
        lines.append("Для выбранного фильтра заказов нет. Смените фильтр или обновите список.")
    else:
        for idx, order in enumerate(orders, start=1):
            number = order.get("number", "-")
            date = order.get("date", "-")
            total = order.get("sum", "0")
            paid = bool(order.get("paid"))
            positions = order.get("positions", []) or []
            first_status = next(
                (p.get("status") for p in positions if p.get("status")),
                "Статус неизвестен",
            )
            lines.append(
                " ".join(
                    [
                        f"{idx}. №{number}",
                        f"• {date}",
                        f"• {emoji_for_status_line(first_status)} {first_status}",
                        f"• {total} ₽",
                        "• ✅ Оплачен" if paid else "• ⏳ Не оплачен",
                    ]
                ).replace("  ", " "),
            )

    if refreshed_at:
        lines.extend(["", f"🔄 Обновлено: {refreshed_at}"])

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
    filter_mode: str = "all",
) -> InlineKeyboardMarkup:
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

    if number_to_token:
        filter_buttons = [
            InlineKeyboardButton(
                ("✅ " if mode == filter_mode else "") + label,
                callback_data=f"orders:filter:{mode}",
            )
            for mode, label in FILTER_MODES.items()
        ]
        buttons.append(filter_buttons)

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


def persist_orders_snapshot(user_id: str | int | None, orders: list[dict]):
    """Сохраняет текущие статусы заказов в базе, чтобы вачдог не рассылал их повторно."""

    if not user_id:
        return

    user_id_str = str(user_id)
    for order in orders:
        number = order.get("number")
        if not number:
            continue

        number_str = str(number)
        text = format_order_status(order)
        try:
            update_order_status(number_str, user_id_str, text)
        except Exception as db_error:
            logger.debug(
                "Не удалось обновить снимок статуса заказа %s: %s", number_str, db_error
            )


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
    await clean_and_reply(
        update.message,
        context,
        "👋 Привет! Отправь свой номер телефона, чтобы получить информацию о заказах.",
        reply_markup=kb,
    )


def build_orders_menu_keyboard(current_filter: str | None = None) -> ReplyKeyboardMarkup:
    """Основная клавиатура под полем ввода с быстрыми действиями."""

    filter_labels = {
        "all": "Фильтр: Все",
        "active": "Фильтр: В работе",
        "unpaid": "Фильтр: Неоплаченные",
    }

    rows = [
        [KeyboardButton("📋 Мои заказы"), KeyboardButton("🔄 Обновить заказы")],
        [
            KeyboardButton(
                ("✅ " if mode == current_filter else "") + label
            )
            for mode, label in filter_labels.items()
        ],
    ]

    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


MENU_HINT_TEXT = "👇 Быстрые действия доступны на клавиатуре ниже."


def remember_cleanup_message(context: ContextTypes.DEFAULT_TYPE, message_id: int):
    cleanup_list = context.user_data.setdefault("cleanup_message_ids", [])
    if message_id not in cleanup_list:
        cleanup_list.append(message_id)


async def clear_user_chat(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    *,
    preserve_ids: set[int] | None = None,
):
    """Удаляет отслеживаемые сообщения бота перед отправкой нового."""

    preserve = set(preserve_ids or set())

    tracked_ids = set(context.user_data.get("cleanup_message_ids", []))
    for key in ("active_message_id", "menu_message_id"):
        msg_id = context.user_data.get(key)
        if msg_id:
            tracked_ids.add(msg_id)

    remaining: list[int] = []
    for msg_id in tracked_ids:
        if msg_id in preserve:
            remaining.append(msg_id)
            continue
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except BadRequest as exc:
            if "message to delete not found" not in str(exc).lower():
                logger.debug(f"Не удалось очистить сообщение {msg_id}: {exc}")
        except Exception as error:
            logger.debug(f"Ошибка при очистке сообщения {msg_id}: {error}")

    context.user_data["cleanup_message_ids"] = remaining

    if "active_message_id" not in preserve:
        context.user_data.pop("active_message_id", None)
    if "menu_message_id" not in preserve:
        context.user_data.pop("menu_message_id", None)


async def clean_and_reply(
    message: Message,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    **kwargs,
):
    """Отправляет ответ, предварительно очищая предыдущие сообщения бота."""

    if not message:
        return None

    chat_id = message.chat.id
    await clear_user_chat(context, chat_id)
    sent = await message.reply_text(text, **kwargs)
    if sent:
        remember_cleanup_message(context, sent.message_id)
    return sent


def normalize_order_number_hint(text: str) -> str | None:
    digits = re.sub(r"\D", "", text or "")
    if len(digits) >= 6:
        return digits
    return None


def parse_sum_hint(text: str) -> Decimal | None:
    if not text:
        return None
    cleaned = text.replace(" ", "").replace(",", ".")
    match = re.search(r"\d+(?:\.\d{1,2})?", cleaned)
    if not match:
        return None
    try:
        return Decimal(match.group())
    except InvalidOperation:
        return None


def parse_decimal_value(raw_value) -> Decimal | None:
    if raw_value is None:
        return None
    text = str(raw_value).strip()
    if not text:
        return None
    text = text.replace(" ", "").replace(",", ".")
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _resolve_user_by_hint(hint_type: str, hint_value):
    """Ищет пользователя по номеру или сумме заказа среди известных аккаунтов."""

    users = get_all_users()
    checked_users: set[str] = set()

    target_number: str | None = None
    if hint_type == "order_number":
        target_number = str(hint_value)
        mapped_user_id = get_user_id_by_order_number(target_number)
        if mapped_user_id:
            orders = get_orders_by_user_id(mapped_user_id) or []
            for order in orders:
                if str(order.get("number")) == target_number:
                    return {
                        "user_id": str(mapped_user_id),
                        "order": order,
                        "order_number": target_number,
                        "match_type": "order_number",
                    }

    target_sum: Decimal | None = None
    if hint_type == "order_sum" and isinstance(hint_value, Decimal):
        target_sum = hint_value

    for entry in users:
        user_id = str(entry.get("user_id") or entry.get("abcp_user_id") or "")
        if not user_id or user_id in checked_users:
            continue
        checked_users.add(user_id)

        orders = get_orders_by_user_id(user_id) or []
        for order in orders:
            order_number = str(order.get("number") or "")
            order_sum_value = parse_decimal_value(order.get("sum"))

            if hint_type == "order_number" and target_number:
                if order_number == target_number:
                    return {
                        "user_id": user_id,
                        "order": order,
                        "order_number": target_number,
                        "account_phone": entry.get("phone"),
                        "match_type": "order_number",
                    }

            if (
                hint_type == "order_sum"
                and target_sum is not None
                and order_sum_value is not None
                and abs(order_sum_value - target_sum) < Decimal("0.01")
            ):
                return {
                    "user_id": user_id,
                    "order": order,
                    "order_sum": target_sum,
                    "account_phone": entry.get("phone"),
                    "match_type": "order_sum",
                }

    return None


async def resolve_user_by_hint(hint_type: str, hint_value):
    return await asyncio.to_thread(_resolve_user_by_hint, hint_type, hint_value)


async def complete_authorization_flow(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    user_profile: dict,
    phone: str,
    account_phone: str | None = None,
    summary_text: str | None = None,
    summary_markup: ReplyKeyboardRemove | None = None,
    prefix_text: str | None = None,
) -> bool:
    message = update.message
    chat = update.effective_chat
    if not chat:
        return False

    user_id = str(user_profile.get("userId") or user_profile.get("user_id") or "")
    if not user_id:
        if message:
            await clean_and_reply(
                message,
                context,
                "⚠️ Не удалось определить пользователя. Обратитесь к менеджеру.",
            )
        return False

    name = user_profile.get("name") or "—"

    if summary_text and message:
        await clean_and_reply(
            message,
            context,
            summary_text,
            reply_markup=summary_markup,
        )

    context.user_data["abcp_user_id"] = user_id
    context.user_data["customer_name"] = name
    context.user_data["phone"] = phone
    context.user_data["account_phone"] = account_phone or phone
    context.user_data["active_chat_id"] = chat.id

    stale_messages = 0
    try:
        snapshots = await asyncio.to_thread(get_user_order_snapshots, user_id)
    except Exception as db_error:
        logger.debug(
            "Не удалось получить сохранённые сообщения заказов пользователя %s: %s",
            user_id,
            db_error,
        )
        snapshots = []

    for snapshot in snapshots:
        order_number = snapshot.get("order_number")
        message_id = snapshot.get("message_id")
        if message_id:
            try:
                await context.bot.delete_message(chat_id=chat.id, message_id=message_id)
                stale_messages += 1
            except BadRequest as exc:
                if "message to delete not found" not in str(exc).lower():
                    logger.debug(
                        "Не удалось удалить сообщение заказа %s (%s): %s",
                        order_number,
                        message_id,
                        exc,
                    )
            except Exception as delete_error:
                logger.debug(
                    "Ошибка при удалении сообщения заказа %s (%s): %s",
                    order_number,
                    message_id,
                    delete_error,
                )
        if order_number:
            try:
                await asyncio.to_thread(clear_order_message, order_number)
            except Exception as db_error:
                logger.debug(
                    "Не удалось сбросить message_id заказа %s: %s",
                    order_number,
                    db_error,
                )

    if stale_messages:
        logger.info(
            "Удалено устаревших уведомлеий по заказам пользователя %s: %s",
            user_id,
            stale_messages,
        )

    synced = await sync_orders_context(context, force_refresh=True)
    if not synced:
        if message:
            await clean_and_reply(
                message,
                context,
                "⚠️ Не удалось загрузить заказы. Попробуйте позже.",
                reply_markup=ReplyKeyboardRemove(),
            )
        return False

    _, _, overview_text, keyboard = synced
    await send_overview_message(
        update,
        context,
        overview_text,
        keyboard,
        prefix_text=prefix_text,
    )
    await refresh_menu_keyboard(context, chat_id=chat.id)

    context.user_data["view"] = "overview"
    save_user(update.effective_user.id, account_phone or phone, user_id)
    logger.info(
        "Пользователь %s (%s) успешно авторизован.",
        name,
        user_id,
    )
    context.user_data.pop("auth_state", None)
    return True


async def attempt_alternative_authorization(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    hint_text: str,
) -> bool:
    auth_state = context.user_data.get("auth_state") or {}
    pending_phone = auth_state.get("phone")
    if not pending_phone:
        return False

    new_phone = extract_phone_number(hint_text)
    if new_phone and new_phone != pending_phone:
        await handle_contact(update, context)
        return True

    order_number = normalize_order_number_hint(hint_text)
    match = None
    if order_number:
        match = await resolve_user_by_hint("order_number", order_number)

    if not match:
        sum_hint = parse_sum_hint(hint_text)
        if sum_hint is not None:
            match = await resolve_user_by_hint("order_sum", sum_hint)

    if not match:
        await clean_and_reply(
            update.message,
            context,
            "❌ Не удалось подтвердить аккаунт по указанным данным. "
            "Отправьте номер заказа (например 234808176) или сумму заказа в рублях.",
        )
        return False

    account_phone = match.get("account_phone") or auth_state.get("account_phone")
    user_profile = None
    if account_phone:
        user_profile = await asyncio.to_thread(get_user_by_phone, account_phone)

    if not user_profile:
        order = match.get("order") or {}
        user_profile = {
            "userId": match.get("user_id"),
            "name": order.get("clientName")
            or order.get("userName")
            or auth_state.get("name")
            or "Клиент",
            "balance": order.get("clientBalance") or order.get("balance") or "—",
            "debt": order.get("clientDebt") or order.get("debt") or "—",
        }

    summary_lines = ["✅ Дополнительная проверка пройдена."]
    if match.get("match_type") == "order_number":
        summary_lines.append(f"📦 Номер заказа: {match.get('order_number')}")
    elif match.get("match_type") == "order_sum":
        summary_lines.append(
            f"💰 Сумма заказа: {match.get('order_sum')} ₽"
        )
    summary_lines.append("⏳ Загружаем список заказов...")
    summary_text = "\n".join(summary_lines)

    success = await complete_authorization_flow(
        update,
        context,
        user_profile=user_profile,
        phone=pending_phone,
        account_phone=account_phone or user_profile.get("phone"),
        summary_text=summary_text,
        summary_markup=ReplyKeyboardRemove(),
    )

    if success:
        await try_delete_message(update.message)
        return True
    return False


async def send_overview_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    overview_text: str,
    keyboard: InlineKeyboardMarkup,
    *,
    prefix_text: str | None = None,
) -> None:
    """Редактирует существующее сообщение со списком заказов или отправляет новое."""

    chat_id = context.user_data.get("active_chat_id") or update.effective_chat.id
    context.user_data["active_chat_id"] = chat_id
    prev_message_id = context.user_data.get("active_message_id")

    full_text = (
        f"{prefix_text}\n\n{overview_text}" if prefix_text else overview_text
    )

    if prev_message_id:
        try:
            await safe_edit_message_text(
                context.bot,
                chat_id,
                prev_message_id,
                full_text,
                reply_markup=keyboard,
            )
            return
        except Exception as edit_error:
            logger.debug(f"Не удалось обновить прошлое сообщение: {edit_error}")
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=prev_message_id)
            except Exception:
                pass

    await clear_user_chat(context, chat_id)

    msg = await context.bot.send_message(
        chat_id=chat_id,
        text=full_text,
        reply_markup=keyboard,
    )
    context.user_data["active_message_id"] = msg.message_id
    remember_cleanup_message(context, msg.message_id)


async def try_delete_message(message: Message | None):
    if not message:
        return
    try:
        await message.delete()
    except BadRequest as exc:
        if "message can't be deleted" in str(exc).lower():
            return
        logger.debug(f"Не удалось удалить сообщение пользователя: {exc}")
    except Exception as error:
        logger.debug(f"Ошибка при удалении сообщения пользователя: {error}")


async def refresh_menu_keyboard(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int | None = None,
    message_text: str | None = None,
):
    """Переотправляет клавиатуру с быстрыми действиями, чтобы обновить чекбоксы."""

    target_chat = chat_id or context.user_data.get("active_chat_id")
    if not target_chat:
        return

    keyboard = build_orders_menu_keyboard(context.user_data.get("orders_filter"))
    text = message_text or MENU_HINT_TEXT
    previous_menu_id = context.user_data.get("menu_message_id")

    if previous_menu_id:
        try:
            await context.bot.delete_message(
                chat_id=target_chat, message_id=previous_menu_id
            )
        except BadRequest as exc:
            if "message to delete not found" not in str(exc).lower():
                logger.debug(f"Не удалось удалить прошлое меню: {exc}")
        except Exception as error:
            logger.debug(f"Ошибка при удалении меню: {error}")

    sent = await context.bot.send_message(
        chat_id=target_chat,
        text=text,
        reply_markup=keyboard,
    )
    context.user_data["menu_message_id"] = sent.message_id
    remember_cleanup_message(context, sent.message_id)


async def sync_orders_context(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    force_refresh: bool = False,
    filter_mode: str | None = None,
) -> tuple[list[dict], list[dict], str, InlineKeyboardMarkup] | None:
    """Актуализирует кеш заказов в user_data и возвращает данные для отображения."""

    user_id = context.user_data.get("abcp_user_id")
    if not user_id:
        return None

    needs_fetch = force_refresh or context.user_data.get("orders_list") is None

    if needs_fetch:
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
        await asyncio.to_thread(persist_orders_snapshot, user_id, orders_list)
        refreshed_at = datetime.now().strftime("%d.%m.%Y %H:%M")
        context.user_data["orders_last_synced"] = refreshed_at
    else:
        orders_list = context.user_data.get("orders_list", [])
        refreshed_at = context.user_data.get("orders_last_synced")
        number_to_token = context.user_data.get("orders_number_to_token", {})
        if not number_to_token and orders_list:
            number_to_token, token_to_number = assign_order_tokens(orders_list)
            context.user_data["orders_number_to_token"] = number_to_token
            context.user_data["orders_token_to_number"] = token_to_number

    metrics = context.user_data.get("orders_metrics") if not needs_fetch else None
    if force_refresh or metrics is None:
        metrics = calculate_orders_metrics(orders_list)
        context.user_data["orders_metrics"] = metrics

    current_filter = filter_mode or context.user_data.get("orders_filter")
    if current_filter not in FILTER_MODES:
        current_filter = "active" if metrics.get("active") else "all"
    context.user_data["orders_filter"] = current_filter

    filtered_orders = filter_orders_for_view(orders_list, current_filter)
    context.user_data["orders_filtered_list"] = filtered_orders

    refreshed_at = context.user_data.get("orders_last_synced")
    overview_text = format_orders_overview(
        filtered_orders,
        metrics,
        current_filter,
        refreshed_at,
    )
    context.user_data["orders_overview_text"] = overview_text

    number_to_token = context.user_data.get("orders_number_to_token", {})
    keyboard = build_orders_keyboard(filtered_orders, number_to_token, current_filter)

    return orders_list, filtered_orders, overview_text, keyboard


async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Авторизация и аккуратное отображение списка заказов."""
    try:
        message = update.message
        if message and message.contact:
            phone = message.contact.phone_number
        else:
            phone = extract_phone_number((message.text if message else "") or "")

        if not phone:
            await clean_and_reply(
                update.message,
                context,
                "❌ Не удалось распознать номер телефона.",
            )
            return

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

        user = await asyncio.to_thread(get_user_by_phone, phone)
        if not user:
            context.user_data["auth_state"] = {
                "step": "await_hint",
                "phone": phone,
            }
            await clean_and_reply(
                update.message,
                context,
                "📭 С этим номером телефона профиль не найден. "
                "Если на сайте указан другой номер, пришлите его или отправьте номер "
                "последнего заказа (например 234808176) либо сумму заказа для подтверждения.",
                reply_markup=ReplyKeyboardRemove(),
            )
            return

        name = user.get("name", "—")
        balance = user.get("balance", "0.00")
        debt = user.get("debt", "0.00")

        summary_text = (
            "✅ Авторизация прошла успешно!\n"
            f"👤 {name}\n"
            f"💰 Баланс: {balance} ₽\n"
            f"💸 Задолженность: {debt} ₽\n\n"
            "⏳ Загружаем список заказов..."
        )

        success = await complete_authorization_flow(
            update,
            context,
            user_profile=user,
            phone=phone,
            account_phone=phone,
            summary_text=summary_text,
            summary_markup=ReplyKeyboardRemove(),
        )

        if success:
            await try_delete_message(update.message)

    except Exception as e:
        logger.exception(f"Ошибка при авторизации или выводе заказов: {e}")
        await clean_and_reply(
            update.message,
            context,
            "⚠️ Ошибка при загрузке заказов. Подробности в логах.",
        )


async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает текстовые сообщения: команды и повторную авторизацию."""

    message = update.message
    if not message:
        return

    text = (message.text or "").strip()
    if not text:
        return

    chat_id = message.chat.id
    context.user_data.setdefault("active_chat_id", chat_id)

    auth_state = context.user_data.get("auth_state") or {}
    if auth_state.get("step") == "await_hint":
        await attempt_alternative_authorization(update, context, text)
        return

    user_id = context.user_data.get("abcp_user_id")

    if not user_id:
        phone = extract_phone_number(text)
        if phone:
            await handle_contact(update, context)
        else:
            await clean_and_reply(
                message,
                context,
                "📱 Отправьте номер телефона или воспользуйтесь кнопкой \"Отправить номер телефона\".",
                reply_markup=ReplyKeyboardMarkup(
                    [[KeyboardButton("Отправить номер телефона", request_contact=True)]],
                    resize_keyboard=True,
                    one_time_keyboard=True,
                ),
            )
        return

    lower_text = text.lower()

    if lower_text in {"📋 мои заказы", "мои заказы", "список заказов", "📋 список заказов"}:
        synced = await sync_orders_context(context, force_refresh=False)
        if not synced:
            await clean_and_reply(
                message,
                context,
                "⚠️ Не удалось получить список заказов. Попробуйте позже.",
            )
            return
        _, _, overview_text, keyboard = synced
        await send_overview_message(update, context, overview_text, keyboard)
        await refresh_menu_keyboard(context)
        await try_delete_message(message)
        return

    if lower_text in {"🔄 обновить", "🔄 обновить заказы", "обновить заказы", "обновить список"}:
        synced = await sync_orders_context(context, force_refresh=True)
        if not synced:
            await clean_and_reply(
                message,
                context,
                "⚠️ Не удалось обновить заказы. Попробуйте позже.",
            )
            return
        _, _, overview_text, keyboard = synced
        await send_overview_message(update, context, overview_text, keyboard)
        await refresh_menu_keyboard(context)
        await try_delete_message(message)
        return

    if lower_text.startswith("фильтр") or "фильтр" in lower_text:
        if "в работе" in lower_text:
            mode = "active"
        elif "неоплач" in lower_text:
            mode = "unpaid"
        else:
            mode = "all"

        synced = await sync_orders_context(context, force_refresh=False, filter_mode=mode)
        if not synced:
            await clean_and_reply(
                message,
                context,
                "⚠️ Не удалось применить фильтр. Попробуйте позже.",
            )
            return
        _, _, overview_text, keyboard = synced
        await send_overview_message(update, context, overview_text, keyboard)
        await refresh_menu_keyboard(context)
        await try_delete_message(message)
        return

    phone_candidate = extract_phone_number(text)
    if phone_candidate and phone_candidate != context.user_data.get("phone"):
        await handle_contact(update, context)
        return

    await refresh_menu_keyboard(
        context,
        message_text="ℹ️ Используйте клавиатуру снизу, чтобы открыть список, обновить его или сменить фильтр.",
    )
    await try_delete_message(message)


async def handle_orders_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка inline-кнопок со списком заказов."""
    query = update.callback_query
    if not query:
        return

    data = (query.data or "").strip()
    chat_id = query.message.chat.id
    context.user_data["active_chat_id"] = chat_id
    context.user_data["active_message_id"] = query.message.message_id

    if data == "orders:back":
        orders = context.user_data.get("orders_list")
        metrics = context.user_data.get("orders_metrics")
        current_filter = context.user_data.get("orders_filter")
        if (
            orders is None
            or metrics is None
            or current_filter not in FILTER_MODES
        ):
            synced = await sync_orders_context(context, force_refresh=True)
            if not synced:
                return
            _, filtered_orders, text_block, keyboard = synced
        else:
            filtered_orders = filter_orders_for_view(orders, current_filter)
            refreshed_at = context.user_data.get("orders_last_synced")
            text_block = format_orders_overview(
                filtered_orders,
                metrics,
                current_filter,
                refreshed_at,
            )
            context.user_data["orders_overview_text"] = text_block
            context.user_data["orders_filtered_list"] = filtered_orders
            keyboard = build_orders_keyboard(
                filtered_orders,
                context.user_data.get("orders_number_to_token", {}),
                current_filter,
            )
        try:
            await safe_edit_query_message(query, text_block, keyboard)
        except Exception as edit_error:
            logger.debug(f"Не удалось показать список заказов: {edit_error}")
        await query.answer()
        await refresh_menu_keyboard(context, chat_id=chat_id)
        context.user_data["view"] = "overview"
        return

    if data == "orders:refresh":
        synced = await sync_orders_context(context, force_refresh=True)
        if not synced:
            return
        _, _, text_block, keyboard = synced
        try:
            await safe_edit_query_message(query, text_block, keyboard)
        except Exception as edit_error:
            logger.debug(f"Не удалось обновить список заказов: {edit_error}")
        await query.answer("Список обновлён ✅", show_alert=False)
        await refresh_menu_keyboard(context, chat_id=chat_id)
        context.user_data["view"] = "overview"
        return

    if data.startswith("orders:filter:"):
        mode = data.split(":", 2)[2]
        if mode not in FILTER_MODES:
            await query.answer("Неизвестный фильтр", show_alert=True)
            return

        current_filter = context.user_data.get("orders_filter")
        if current_filter == mode:
            await query.answer("Этот фильтр уже активен ✅", show_alert=False)
            return

        orders = context.user_data.get("orders_list")
        if orders is None:
            synced = await sync_orders_context(context, force_refresh=True)
            if not synced:
                return
            orders = context.user_data.get("orders_list", [])

        metrics = context.user_data.get("orders_metrics")
        if metrics is None:
            metrics = calculate_orders_metrics(orders)
            context.user_data["orders_metrics"] = metrics

        context.user_data["orders_filter"] = mode
        filtered_orders = filter_orders_for_view(orders, mode)
        refreshed_at = context.user_data.get("orders_last_synced")
        overview_text = format_orders_overview(
            filtered_orders,
            metrics,
            mode,
            refreshed_at,
        )
        context.user_data["orders_overview_text"] = overview_text
        context.user_data["orders_filtered_list"] = filtered_orders
        keyboard = build_orders_keyboard(
            filtered_orders,
            context.user_data.get("orders_number_to_token", {}),
            mode,
        )
        try:
            await safe_edit_query_message(query, overview_text, keyboard)
        except Exception as edit_error:
            logger.debug(f"Не удалось применить фильтр заказов: {edit_error}")
        await query.answer("Фильтр применён ✅", show_alert=False)
        await refresh_menu_keyboard(context, chat_id=chat_id)
        context.user_data["view"] = "overview"
        return

    if data.startswith("order-refresh:"):
        token = data.split(":", 1)[1]
        token_to_number = context.user_data.get("orders_token_to_number", {})
        number = token_to_number.get(token)
        if not number:
            synced = await sync_orders_context(context, force_refresh=True)
            if not synced:
                return
            token_to_number = context.user_data.get("orders_token_to_number", {})
            number = token_to_number.get(token)
        if not number:
            await query.answer("Заказ не найден", show_alert=True)
            return
        synced = await sync_orders_context(context, force_refresh=True)
        if not synced:
            return
        order = context.user_data.get("orders_map", {}).get(number)
        if not order:
            current_filter = context.user_data.get("orders_filter", "all")
            keyboard = build_orders_keyboard(
                context.user_data.get("orders_filtered_list", []),
                context.user_data.get("orders_number_to_token", {}),
                current_filter,
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
            synced = await sync_orders_context(context, force_refresh=True)
            if not synced:
                return
            token_to_number = context.user_data.get("orders_token_to_number", {})
            number = token_to_number.get(token)
        if not number:
            current_filter = context.user_data.get("orders_filter", "all")
            keyboard = build_orders_keyboard(
                context.user_data.get("orders_filtered_list", []),
                context.user_data.get("orders_number_to_token", {}),
                current_filter,
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
            synced = await sync_orders_context(context, force_refresh=True)
            if not synced:
                return
            order = context.user_data.get("orders_map", {}).get(number)

        if not order:
            current_filter = context.user_data.get("orders_filter", "all")
            keyboard = build_orders_keyboard(
                context.user_data.get("orders_filtered_list", []),
                context.user_data.get("orders_number_to_token", {}),
                current_filter,
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
def build_changes_actions_for_user(user: dict) -> list[dict]:
    """Готовит действия по уведомлению пользователя об изменениях заказов."""

    chat_id = user.get("telegram_id") or user.get("chat_id")
    user_id = user.get("user_id") or user.get("abcp_user_id")
    if not chat_id or not user_id:
        logger.warning(f"⛔ Пропущен пользователь (нет chat_id или user_id): {user}")
        return []

    try:
        orders = get_orders_by_user_id(user_id) or []
    except Exception as e:
        logger.exception(f"[WD] Не удалось получить заказы пользователя {user_id}: {e}")
        return []

    actions: list[dict] = []
    cache_dirty = False

    for order in orders:
        number = order.get("number")
        if number is None:
            continue

        number_str = str(number)
        fresh_text = format_order_status(order)
        cache_text = _status_cache.get(number_str)
        stored_status = get_order_status(number_str)
        stored_message_id = get_order_message(number_str)

        if fresh_text == cache_text or fresh_text == stored_status:
            if cache_text != fresh_text:
                _status_cache[number_str] = fresh_text
                cache_dirty = True
            continue

        # Если в базе нет форматированного статуса (новый заказ или старые записи),
        # фиксируем снапшот и не рассылаем уведомление, чтобы избежать спама.
        if not stored_status or "Заказ №" not in stored_status:
            try:
                update_order_status(
                    number_str,
                    user_id,
                    fresh_text,
                    stored_message_id,
                )
            except Exception as db_error:
                logger.debug(
                    "[WD] Не удалось зафиксировать стартовый статус заказа %s: %s",
                    number_str,
                    db_error,
                )
            _status_cache[number_str] = fresh_text
            cache_dirty = True
            continue

        actions.append(
            {
                "chat_id": chat_id,
                "user_id": user_id,
                "order_number": number_str,
                "message_id": stored_message_id,
                "text": f"📢 Обновление статусов:\n\n{fresh_text}",
                "raw_text": fresh_text,
            }
        )

    if cache_dirty:
        save_cache()

    return actions


async def watchdog_job(context: ContextTypes.DEFAULT_TYPE):
    """Запускается JobQueue-ой каждые 60 сек внутри event-loop приложения."""
    try:
        users = get_all_users()
        if not users:
            logger.info("[WD] Пользователей нет.")
            return

        total_updates = 0
        for u in users:
            actions = await asyncio.to_thread(build_changes_actions_for_user, u)
            for action in actions:
                chat_id = action["chat_id"]
                order_number = action["order_number"]
                raw_text = action["raw_text"]
                message_id = action.get("message_id")

                try:
                    new_message_id = message_id
                    if message_id:
                        try:
                            await context.bot.edit_message_text(
                                chat_id=chat_id,
                                message_id=message_id,
                                text=action["text"],
                            )
                        except BadRequest as exc:
                            logger.debug(
                                f"[WD] Не удалось обновить сообщение заказа {order_number}: {exc}. Переотправляем."
                            )
                            new_message_id = None
                    if not new_message_id:
                        sent = await context.bot.send_message(
                            chat_id=chat_id, text=action["text"]
                        )
                        new_message_id = sent.message_id

                    await asyncio.to_thread(
                        update_order_status,
                        order_number,
                        action["user_id"],
                        raw_text,
                        new_message_id,
                    )
                    _status_cache[order_number] = raw_text
                    total_updates += 1
                except Exception as e:
                    logger.exception(
                        f"[WD] Ошибка обработки уведомления по заказу {order_number} для пользователя {chat_id}: {e}"
                    )

        if total_updates:
            save_cache()
            logger.info(f"[WD] Обновлено уведомлений: {total_updates}")
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
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))

    # Запускаем вачдог через JobQueue каждые 60 сек (первый запуск через 10 сек)
    app.job_queue.run_repeating(watchdog_job, interval=60, first=10, name="orders_watchdog")

    logger.info("🤖 Бот запущен и готов к работе (watchdog: 60 сек).")
    app.run_polling()
