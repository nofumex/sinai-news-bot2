import asyncio
import time

import httpx


class APIError(Exception):
    def __init__(self, service, status=0, code="error", retry_after=0, uncertain=False):
        self.service, self.status, self.code = service, status, code
        self.retry_after, self.uncertain = retry_after, uncertain
        super().__init__(f"{service}: HTTP {status}, {code}")

    @property
    def retryable(self):
        return self.status == 0 or self.status == 429 or self.status >= 500 or self.code == "attachment.not.ready"


class RateLimit:
    def __init__(self, rps):
        self.interval = 1 / rps
        self.lock = asyncio.Lock()
        self.next_at = 0.0

    async def wait(self):
        async with self.lock:
            now = time.monotonic()
            if self.next_at > now:
                await asyncio.sleep(self.next_at - now)
            self.next_at = time.monotonic() + self.interval


async def request(client, service, method, url, **kwargs):
    try:
        r = await client.request(method, url, **kwargs)
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
        raise APIError(service, code="connection") from None
    except httpx.HTTPError:
        raise APIError(service, code="transport", uncertain=method != "GET") from None
    try:
        data = r.json() if r.content else {}
    except ValueError:
        if r.is_success:
            raise APIError(service, r.status_code, "invalid_response", uncertain=method != "GET") from None
        data = {}
    status = r.status_code
    if service == "telegram" and data.get("ok") is False:
        status = data.get("error_code", status)
    if status >= 400:
        delay = (data.get("parameters") or {}).get("retry_after", r.headers.get("Retry-After", 0))
        try:
            delay = float(delay)
        except (TypeError, ValueError):
            delay = 30
        # Do not include response bodies/URLs: they can contain tokens and PII.
        code = str(data.get("code", "rejected"))[:80]
        raise APIError(service, status, code, delay, uncertain=method != "GET" and status >= 500)
    return data

