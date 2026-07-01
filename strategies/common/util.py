import datetime

def safe_float(val, default=0.0):
    try:
        if val is None: return default
        return float(val)
    except:
        return default

def utc_now_iso():
    return datetime.datetime.utcnow().isoformat()
