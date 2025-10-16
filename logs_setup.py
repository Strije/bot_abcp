import logging, os, time
def setup_logging(log_file_path: str):
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    now = time.time(); seven_days = 7*24*60*60
    for name in os.listdir(os.path.dirname(log_file_path)):
        p = os.path.join(os.path.dirname(log_file_path), name)
        try:
            if os.path.isfile(p) and now - os.path.getmtime(p) > seven_days:
                os.remove(p)
        except Exception:
            pass
    logger = logging.getLogger("bot"); logger.setLevel(logging.INFO); logger.handlers.clear()
    ffmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(log_file_path, encoding="utf-8"); fh.setLevel(logging.INFO); fh.setFormatter(ffmt)
    ch = logging.StreamHandler(); ch.setLevel(logging.INFO); ch.setFormatter(ffmt)
    logger.addHandler(fh); logger.addHandler(ch)
    return logger
