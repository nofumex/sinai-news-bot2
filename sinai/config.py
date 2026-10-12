from dataclasses import dataclass
import os
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    amo_url: str
    amo_token: str
    tg_token: str
    max_token: str
    admin_id: int
    max_admin_id: int
    db_path: str = "data/sinai.sqlite3"
    media_dir: str = "data/media"
    max_url: str = "https://platform-api2.max.ru"
    timezone: str = "Asia/Krasnoyarsk"
    completed_pipeline: int = 8993570
    completed_interval: int = 2
    sync_seconds: int = 300
    media_bytes: int = 20 * 1024 * 1024
    ca_bundle: str = ""
    client_id: str = ""
    client_secret: str = ""
    refresh_token: str = ""
    redirect_uri: str = ""

    @classmethod
    def load(cls):
        load_dotenv(encoding="utf-8-sig")
        def required(key):
            value = os.getenv(key, "").strip()
            if not value:
                raise ValueError(f"Не заполнена настройка {key}")
            return value
        cfg = cls(
            amo_url=required("AMOCRM_BASE_URL").rstrip("/"), amo_token=required("AMOCRM_ACCESS_TOKEN"),
            tg_token=required("TG_BOT_TOKEN"), max_token=required("MAX_BOT_TOKEN"),
            admin_id=int(required("ADMIN_ID")), max_admin_id=int(required("MAX_ADMIN_ID")),
            db_path=os.getenv("DATABASE_PATH", "data/sinai.sqlite3"),
            media_dir=os.getenv("MEDIA_DIR", "data/media"),
            max_url=os.getenv("MAX_BASE_URL", "https://platform-api2.max.ru").rstrip("/"),
            timezone=os.getenv("TIMEZONE", "Asia/Krasnoyarsk"),
            completed_pipeline=int(os.getenv("COMPLETED_PIPELINE_ID", "8993570")),
            completed_interval=int(os.getenv("COMPLETED_INTERVAL_MONTHS", "2")),
            sync_seconds=int(os.getenv("CRM_SYNC_SECONDS", "300")),
            media_bytes=int(os.getenv("MAX_MEDIA_BYTES", str(20 * 1024 * 1024))),
            ca_bundle=os.getenv("TLS_CA_BUNDLE", ""),
            client_id=os.getenv("AMOCRM_CLIENT_ID", ""), client_secret=os.getenv("AMOCRM_CLIENT_SECRET", ""),
            refresh_token=os.getenv("AMOCRM_REFRESH_TOKEN", ""), redirect_uri=os.getenv("AMOCRM_REDIRECT_URI", ""),
        )
        if cfg.completed_interval not in (1, 2) or cfg.sync_seconds < 30 or cfg.media_bytes < 1:
            raise ValueError("Неверные интервалы или лимит файлов")
        if not cfg.amo_url.startswith("https://") or not cfg.max_url.startswith("https://"):
            raise ValueError("API требует HTTPS")
        ZoneInfo(cfg.timezone)
        for p in (Path(cfg.db_path).parent, Path(cfg.media_dir)):
            p.mkdir(parents=True, exist_ok=True)
        return cfg

