import logging, requests
from config import API_URL_USERS, API_URL_ORDERS, USERLOGIN, USERPSW, STATUS_CODES
logger = logging.getLogger("bot")
def format_status(status_code):
    if status_code is None: return "Неизвестный статус"
    try:
        code_str = str(int(status_code))
    except Exception:
        code_str = str(status_code).strip()
    return STATUS_CODES.get(code_str, f"Неизвестный статус ({code_str})")
def get_user_by_phone(phone: str):
    url = API_URL_USERS
    try:
        full_url = f"{url}?userlogin={USERLOGIN}&userpsw={USERPSW}&phone={phone}"
        logger.info(f"→ [API users] GET {full_url}")
        resp = requests.get(url, params={"userlogin": USERLOGIN, "userpsw": USERPSW, "phone": phone}, timeout=10)
        logger.info(f"← [API users] status={resp.status_code}, len={len(resp.text)}")
        if resp.status_code == 200:
            try:
                data = resp.json()
                logger.info(f"[API users] JSON parsed: {len(data) if isinstance(data, list) else 'n/a'}")
                if isinstance(data, list) and data: return data[0]
                return None
            except Exception as e:
                logger.error(f"[API users] JSON parse error: {e} — {resp.text[:400]}"); return None
        logger.error(f"[API users] HTTP {resp.status_code}: {resp.text[:400]}"); return None
    except Exception as e:
        logger.exception(f"[API users] Request error: {e}"); return None
def get_orders_by_user_id(user_id: str):
    url = API_URL_ORDERS
    try:
        full_url = f"{url}?userlogin={USERLOGIN}&userpsw={USERPSW}&userId={user_id}"
        logger.info(f"→ [API orders] GET {full_url}")
        resp = requests.get(url, params={"userlogin": USERLOGIN, "userpsw": USERPSW, "userId": user_id}, timeout=15)
        logger.info(f"← [API orders] status={resp.status_code}, len={len(resp.text)}")
        if resp.status_code == 200:
            try:
                data = resp.json()
                if isinstance(data, list):
                    logger.info(f"[API orders] JSON parsed: {len(data)} orders"); return data
                logger.warning(f"[API orders] Unexpected JSON type: {type(data)}"); return []
            except Exception as e:
                logger.error(f"[API orders] JSON parse error: {e} — {resp.text[:400]}"); return []
        logger.error(f"[API orders] HTTP {resp.status_code}: {resp.text[:400]}"); return []
    except Exception as e:
        logger.exception(f"[API orders] Request error: {e}"); return []
