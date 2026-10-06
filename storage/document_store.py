"""
Document Store: Lưu trữ và trích xuất từng tài liệu vào thư mục riêng biệt theo doc_id.
Kiến trúc 2-Stage Spooler (Tối ưu NAS / Network Filesystem):
  1. Staging Buffer (/tmp): Cào cực nhanh vào ổ cục bộ /tmp
  2. Mover Worker: Cứ sau 10 URLs, tự động di chuyển (mv) sang thư mục project trên NAS
     và xóa sạch dữ liệu đệm ở /tmp để chống tràn ổ đĩa.
"""

import os
import json
import shutil
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List

from ..models import CrawlResult
from ..config import DOCUMENTS_DIR, STAGING_DIR, USE_STAGING_BUFFER

logger = logging.getLogger("vibio_crawler.document_store")

class DocumentStore:
    """Quản lý ghi và đọc tài liệu độc lập theo ID thư mục kèm cơ chế Staging Spooler."""

    def __init__(
        self,
        base_dir: Path = DOCUMENTS_DIR,
        staging_dir: Optional[Path] = None,
        use_staging: bool = USE_STAGING_BUFFER
    ):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        self.use_staging = use_staging
        self.staging_dir = Path(staging_dir) if staging_dir else STAGING_DIR
        if self.use_staging:
            self.staging_dir.mkdir(parents=True, exist_ok=True)
            # Tự động dọn/chuyển các folder còn sót từ phiên chạy trước sang base_dir
            self.flush_remaining_staging()

    def save_document(self, result: CrawlResult) -> Optional[Path]:
        """
        Lưu tài liệu thành công vào thư mục con mang tên docs_id.
        Nếu bật staging: lưu vào staging_dir (/tmp) với tốc độ cao.
        Nếu tắt staging: lưu trực tiếp vào base_dir (NAS/project).
        """
        if not result.is_valid_document():
            return None

        target_dir = self.staging_dir if self.use_staging else self.base_dir
        doc_dir = target_dir / str(result.id)
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

    def move_document_to_final(self, doc_id: int) -> bool:
        """
        Di chuyển 1 thư mục tài liệu từ staging_dir sang base_dir và xóa ở staging_dir.
        Xử lý an toàn cho liên kết khác phân vùng (cross-filesystem mount) giữa /tmp và NAS.
        """
        src = self.staging_dir / str(doc_id)
        if not src.exists():
            return False

        dst = self.base_dir / str(doc_id)
        try:
            # Nếu dst đã tồn tại, xóa trước để tránh shutil.move tạo thư mục con lồng nhau
            if dst.exists():
                shutil.rmtree(dst, ignore_errors=True)
            shutil.move(str(src), str(dst))
            return True
        except Exception as e:
            logger.warning(f"shutil.move gặp lỗi {e}, kích hoạt copytree fallback cho doc_id={doc_id}")
            try:
                shutil.copytree(str(src), str(dst), dirs_exist_ok=True)
                shutil.rmtree(src, ignore_errors=True)
                return True
            except Exception as err:
                logger.error(f"Fallback move thất bại cho doc_id={doc_id}: {err}")
                return False

    def move_batch_to_destination(self, doc_ids: List[int]) -> int:
        """
        Di chuyển một batch tài liệu từ staging sang base_dir và giải phóng đĩa tmp.
        """
        moved = 0
        for doc_id in doc_ids:
            if self.move_document_to_final(doc_id):
                moved += 1
        return moved

    def flush_remaining_staging(self) -> int:
        """
        Quét sạch staging_dir, di chuyển mọi thư mục còn tồn đọng sang base_dir.
        Giúp giải phóng 100% ổ đĩa và tránh thất thoát dữ liệu.
        """
        if not self.use_staging or not self.staging_dir.exists():
            return 0

        moved = 0
        try:
            for entry in list(self.staging_dir.iterdir()):
                if entry.is_dir() and entry.name.isdigit():
                    if self.move_document_to_final(int(entry.name)):
                        moved += 1
        except Exception as e:
            logger.error(f"Lỗi khi dọn dẹp staging_dir: {e}")
        return moved

    def get_document(self, doc_id: int) -> Optional[Dict[str, Any]]:
        """
        Đọc nội dung và metadata của tài liệu từ ổ đĩa theo doc_id.
        Hỗ trợ tra cứu trong base_dir và staging_dir (nếu chưa kịp chuyển).
        """
        doc_dir = self.base_dir / str(doc_id)
        if not doc_dir.exists() and self.use_staging:
            staging_candidate = self.staging_dir / str(doc_id)
            if staging_candidate.exists():
                doc_dir = staging_candidate

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
