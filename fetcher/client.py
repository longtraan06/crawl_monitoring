"""
High-performance asynchronous HTTP client using curl_cffi with Chrome impersonation.
"""

import asyncio
import logging
from typing import Tuple, Optional
from curl_cffi.requests import AsyncSession
from curl_cffi.requests.exceptions import RequestException

from ..config import (
    IMPERSONATE_BROWSER,
    DEFAULT_HEADERS,
    REQUEST_TIMEOUT,
    MAX_RETRIES,
    RETRY_BACKOFF_FACTOR
)
from .handlers import DomainHandlerRegistry

logger = logging.getLogger("vibio_crawler.fetcher")

class AsyncFetcher:
    """Fetcher bất đồng bộ tối ưu hóa cho crawl bài viết y tế quy mô lớn."""

    def __init__(
        self,
        timeout: float = REQUEST_TIMEOUT,
        max_retries: int = MAX_RETRIES,
        max_clients: int = 150
    ):
        self.timeout = timeout
        self.max_retries = max_retries
        self.max_clients = max_clients
        self.session: Optional[AsyncSession] = None
        self._laodong_cookies: Optional[str] = None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def start(self):
        """Khởi tạo session bất đồng bộ với cấu hình Chrome 120 impersonate."""
        if self.session is None:
            self.session = AsyncSession(
                impersonate=IMPERSONATE_BROWSER,
                timeout=self.timeout,
                headers=DEFAULT_HEADERS,
                verify=False,  # Cho phép các cổng y tế cũ chứng chỉ SSL hết hạn
                max_clients=self.max_clients
            )

    async def close(self):
        """Đóng session giải phóng tài nguyên."""
        if self.session is not None:
            await self.session.close()
            self.session = None

    async def fetch(self, url: str, domain: str) -> Tuple[int, str, Optional[str]]:
        """
        Thực hiện tải nội dung HTML với cơ chế retry và tự động vượt challenge.
        Trả về: (status_code, html_content, error_message)
        """
        if self.session is None:
            await self.start()

        custom_headers = DomainHandlerRegistry.get_custom_headers(domain)

        # Xử lý cookie riêng nếu có sẵn cho Lao Động
        if domain == "laodong.vn" and self._laodong_cookies:
            custom_headers["Cookie"] = self._laodong_cookies

        for attempt in range(self.max_retries + 1):
            try:
                resp = await self.session.get(url, headers=custom_headers if custom_headers else None)
                status_code = resp.status_code
                html = resp.text or ""

                # Kiểm tra trang xác thực JS của Lao Động
                if domain == "laodong.vn" and "D1N=" in html and "document.cookie" in html:
                    d1n_cookie = DomainHandlerRegistry.extract_laodong_cookie(html)
                    if d1n_cookie:
                        self._laodong_cookies = d1n_cookie
                        retry_headers = {"Cookie": d1n_cookie}
                        retry_resp = await self.session.get(url, headers=retry_headers)
                        return retry_resp.status_code, retry_resp.text or "", None

                if status_code == 200:
                    return 200, html, None

                # Nếu bị 429 hoặc 5xx thì thử retry
                if status_code in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                    backoff = RETRY_BACKOFF_FACTOR * (attempt + 1)
                    await asyncio.sleep(backoff)
                    continue

                return status_code, html, f"HTTP {status_code}"

            except asyncio.TimeoutError:
                if attempt < self.max_retries:
                    await asyncio.sleep(RETRY_BACKOFF_FACTOR * (attempt + 1))
                    continue
                return 0, "", "Timeout > 18s"

            except RequestException as e:
                err_msg = str(e)
                if "curl: (28)" in err_msg or "Timeout" in err_msg:
                    if attempt < self.max_retries:
                        await asyncio.sleep(RETRY_BACKOFF_FACTOR * (attempt + 1))
                        continue
                    return 0, "", "Timeout"
                return 0, "", f"RequestError: {type(e).__name__}"

            except Exception as e:
                return 0, "", f"UnexpectedError: {type(e).__name__}"

        return 0, "", "Exceeded max retries"
