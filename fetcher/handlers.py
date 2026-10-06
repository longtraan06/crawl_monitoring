"""
Domain-specific request/response handlers and interceptors.
"""

import re
from typing import Optional, Dict

class DomainHandlerRegistry:
    """Điều phối các bộ xử lý đặc biệt cho từng domain cụ thể."""

    @staticmethod
    def extract_laodong_cookie(html: str) -> Optional[str]:
        """Trích xuất cookie xác thực D1N từ trang thử thách JavaScript của Lao Động."""
        match = re.search(r'document\.cookie\s*=\s*["\'](D1N=[^;"\']+)["\']', html)
        if match:
            return match.group(1)
        return None

    @staticmethod
    def get_custom_headers(domain: str) -> Dict[str, str]:
        """Tùy chỉnh headers riêng cho từng domain nếu cần."""
        headers = {}
        if domain == "vnexpress.net":
            headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
            headers["Accept-Language"] = "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7"
        return headers
