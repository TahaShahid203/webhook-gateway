from .models import Control


def get(key: str, default: str = "") -> str:
    c = Control.objects.filter(key=key).first()
    return c.value if c else default


def put(key: str, value: str) -> None:
    Control.objects.update_or_create(key=key, defaults={"value": value})


def ingest_paused() -> bool:
    return get("ingest_paused", "0") == "1"
