import re
from db import add_user
PHONE_REGEX = r"(\+7|7|8)?\D?(\d{3})\D?(\d{3})\D?(\d{2})\D?(\d{2})"
def extract_phone_number(text: str) -> str | None:
    if not text:
        return None
    m = re.search(PHONE_REGEX, text)
    if not m:
        return None
    phone = "".join(m.groups(default=""))
    phone = phone.lstrip("+")
    if phone.startswith("8"):
        phone = "7" + phone[1:]
    elif not phone.startswith("7"):
        phone = "7" + phone
    return phone
def save_user(telegram_id: int, phone: str, user_id: str):
    add_user(telegram_id, phone, user_id)
