"""
Document Store: Lưu trữ và trích xuất từng tài liệu vào thư mục riêng biệt theo doc_id.
Cấu trúc:
  crawler_output/documents/{doc_id}/
      ├── content.md
      └── metadata.json
"""

import os
import json
import logging
from pathlib import Path
from typing import Optional, Dict, Any

from ..models import CrawlResult
from ..config import DOCUMENTS_DIR

logger = logging.getLogger("vibio_crawler.document_store")

class DocumentStore:
    """Quản lý ghi và đọc tài liệu độc lập theo ID thư mục."""

    def __init__(self, base_dir: Path = DOCUMENTS_DIR):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def save_document(self, result: CrawlResult) -> Optional[Path]:
        """
        Lưu tài liệu thành công vào thư mục con mang tên docs_id.
        Tạo file content.md và metadata.json.
        """
        if not result.is_valid_document():
            return None

        doc_dir = self.base_dir / str(result.id)
        doc_dir.mkdir(parents=True, exist_ok=True)

        # 1. Ghi file content.md
        md_file = doc_dir / "content.md"
        with open(md_file, "w", encoding="utf-8") as f:
            f.write(result.markdown)

        # 2. Ghi file metadata.json
        meta_file = doc_dir / "metadata.json"
        metadata = {
            "id": result.id,
            "url": result.url,
            "domain": result.domain,
            "group_id": result.group_id,
            "title": result.title,
            "char_count": result.char_count,
            "crawled_at": result.crawled_at,
            "status": result.status
        }
        with open(meta_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

        return doc_dir

    def get_document(self, doc_id: int) -> Optional[Dict[str, Any]]:
        """Đọc nội dung và metadata của tài liệu từ ổ đĩa theo doc_id."""
        doc_dir = self.base_dir / str(doc_id)
        if not doc_dir.exists():
            return None

        md_file = doc_dir / "content.md"
        meta_file = doc_dir / "metadata.json"

        content_md = ""
        if md_file.exists():
            with open(md_file, "r", encoding="utf-8", errors="replace") as f:
                content_md = f.read()

        metadata = {}
        if meta_file.exists():
            try:
                with open(meta_file, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
            except Exception:
                pass

        return {
            "id": doc_id,
            "path": str(doc_dir),
            "content_md": content_md,
            "metadata": metadata,
            "title": metadata.get("title", ""),
            "url": metadata.get("url", ""),
            "domain": metadata.get("domain", ""),
            "char_count": metadata.get("char_count", len(content_md)),
            "crawled_at": metadata.get("crawled_at", "")
        }
