"""
Data models for the crawling pipeline.
"""

from dataclasses import dataclass, asdict
from typing import Optional
from datetime import datetime

@dataclass
class CrawlTask:
    """Đại diện cho một URL cần crawl từ corpus."""
    id: int
    url: str
    domain: str
    group_id: int = 1
    retry_count: int = 0

@dataclass
class CrawlResult:
    """Kết quả sau khi fetch và trích xuất nội dung bài viết."""
    id: int
    url: str
    domain: str
    status: str  # SUCCESS, EMPTY, HTTP_ERROR, TIMEOUT, FAILED
    group_id: int = 1
    title: str = ""
    markdown: str = ""
    char_count: int = 0
    error: Optional[str] = None
    crawled_at: str = ""

    def __post_init__(self):
        if not self.crawled_at:
            self.crawled_at = datetime.utcnow().isoformat()

    def is_valid_document(self, min_chars: int = 60) -> bool:
        """Kiểm tra tài liệu có đủ điều kiện lưu vào corpus hay không."""
        return self.status == "SUCCESS" and self.char_count >= min_chars and bool(self.markdown.strip())

    def to_record(self) -> dict:
        """Xuất bản ghi sạch cho corpus lưu trữ (JSONL/Parquet)."""
        return {
            "id": self.id,
            "url": self.url,
            "domain": self.domain,
            "title": self.title,
            "markdown": self.markdown,
            "char_count": self.char_count,
            "crawled_at": self.crawled_at
        }
