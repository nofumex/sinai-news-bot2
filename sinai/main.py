import argparse
import asyncio
import logging
import os
from pathlib import Path
import signal
import ssl
import sys

from filelock import FileLock, Timeout
import httpx

from .config import Config
from .crm import AmoCRM
from .engine import Engine
from .http import APIError
from .platforms import Platforms
from .store import Store


async def run(cfg):
    context = ssl.create_default_context(cafile=cfg.ca_bundle or None)
    limits = httpx.Limits(max_connections=15, max_keepalive_connections=10)
    async with httpx.AsyncClient(verify=context, timeout=20, limits=limits, trust_env=False) as client, \
            httpx.AsyncClient(verify=context, timeout=45, trust_env=False) as poll_client:
        store = Store(cfg.db_path)
        try:
            platforms = Platforms(cfg, client, poll_client)
            # Read-only verification of identity and absence of webhooks before receiving updates.
            await platforms.preflight()
            engine = Engine(cfg, store, platforms, AmoCRM(cfg, store, client))
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, engine.stop.set)
                except (NotImplementedError, RuntimeError):
                    pass  # Windows Ctrl-C handled by asyncio.run.
            await engine.run()
        finally:
            store.close()


def cli():
    parser = argparse.ArgumentParser(description="Синай · Telegram/MAX polling bot")
    parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    # HTTP libraries must never log token-containing URLs.
    logging.getLogger("httpx").setLevel(logging.CRITICAL)
    logging.getLogger("httpcore").setLevel(logging.CRITICAL)
    try:
        cfg = Config.load()
        logging.getLogger().setLevel(os.getenv('LOG_LEVEL', 'INFO').upper())
        lock = FileLock(str(Path(cfg.db_path).resolve()) + ".lock", timeout=0)
        with lock:
            asyncio.run(run(cfg))
    except Timeout:
        logging.error("Бот уже запущен с этой БД")
        sys.exit(1)
    except APIError as exc:
        logging.error("Запуск не выполнен: %s", exc)
        sys.exit(1)
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        # Exception strings may carry request URL secrets; report type only.
        logging.error("Остановка из-за ошибки %s", type(exc).__name__)
        sys.exit(1)


if __name__ == "__main__":
    cli()

