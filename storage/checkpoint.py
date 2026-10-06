"""
High-performance SQLite checkpoint tracker with WAL mode and in-memory cache.
Manages status, errors, and indexing for seamless resume and Web Dashboard analytics.
"""

import sqlite3
import logging
from pathlib import Path
from typing import Set, List, Dict, Any, Optional, Tuple

from ..models import CrawlResult
from ..config import CHECKPOINT_DB_PATH

logger = logging.getLogger("vibio_crawler.checkpoint")

class CheckpointTracker:
    """Quản lý trạng thái và checkpoint lưu vết quá trình crawl."""

    def __init__(self, db_path: Path = CHECKPOINT_DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._visited_ids: Set[int] = set()
        self._init_db()
        self._load_cache()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _init_db(self):
        """Khởi tạo cấu trúc bảng SQLite, tự động migrate cột mới và tạo chỉ mục."""
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS crawl_checkpoint (
                    id INTEGER PRIMARY KEY,
                    url TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    group_id INTEGER DEFAULT 1,
                    status TEXT NOT NULL,
                    error TEXT,
                    char_count INTEGER DEFAULT 0,
                    crawled_at TEXT NOT NULL
                );
            """)

            # Tự động migrate thêm cột nếu bảng đã tồn tại từ trước
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(crawl_checkpoint);")
            columns = {row["name"] for row in cursor.fetchall()}
            if "group_id" not in columns:
                conn.execute("ALTER TABLE crawl_checkpoint ADD COLUMN group_id INTEGER DEFAULT 1;")
            if "error" not in columns:
                conn.execute("ALTER TABLE crawl_checkpoint ADD COLUMN error TEXT;")

            conn.execute("CREATE INDEX IF NOT EXISTS idx_checkpoint_domain ON crawl_checkpoint(domain);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_checkpoint_status ON crawl_checkpoint(status);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_checkpoint_group ON crawl_checkpoint(group_id);")

    def _load_cache(self):
        """Tải toàn bộ ID đã crawl vào bộ nhớ RAM để tra cứu tức thì O(1)."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM crawl_checkpoint;")
            rows = cursor.fetchall()
            self._visited_ids = {row[0] for row in rows}
        logger.info(f"Đã nạp {len(self._visited_ids):,} URLs đã crawl từ checkpoint cache.")

    def is_visited(self, doc_id: int) -> bool:
        """Kiểm tra nhanh xem ID tài liệu đã được crawl trước đó hay chưa."""
        return doc_id in self._visited_ids

    def record_batch(self, results: List[CrawlResult]):
        """Ghi nhận hàng loạt kết quả crawl vào SQLite theo transaction atomic."""
        if not results:
            return

        records = [
            (
                res.id,
                res.url,
                res.domain,
                res.group_id,
                res.status,
                res.error or "",
                res.char_count,
                res.crawled_at
            )
            for res in results
        ]

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany("""
                INSERT OR REPLACE INTO crawl_checkpoint (id, url, domain, group_id, status, error, char_count, crawled_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """, records)

        # Cập nhật cache RAM
        for res in results:
            self._visited_ids.add(res.id)

    def get_stats(self) -> Dict[str, Any]:
        """Tổng hợp các chỉ số thống kê tiến độ hiện tại."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*), SUM(char_count) FROM crawl_checkpoint;")
            row = cursor.fetchone()
            total_records = row[0] or 0
            total_chars = row[1] or 0

            cursor.execute("SELECT status, COUNT(*) FROM crawl_checkpoint GROUP BY status;")
            status_counts = {r[0]: r[1] for r in cursor.fetchall()}

            cursor.execute("SELECT group_id, COUNT(*) FROM crawl_checkpoint GROUP BY group_id;")
            group_counts = {r[0]: r[1] for r in cursor.fetchall()}

            cursor.execute("""
                SELECT domain, COUNT(*) as cnt 
                FROM crawl_checkpoint 
                WHERE status = 'SUCCESS' 
                GROUP BY domain 
                ORDER BY cnt DESC 
                LIMIT 10;
            """)
            top_domains = [(r[0], r[1]) for r in cursor.fetchall()]

        return {
            "total_visited": total_records,
            "total_chars": total_chars,
            "status_breakdown": status_counts,
            "group_breakdown": group_counts,
            "top_domains_success": top_domains
        }

    def get_failed_urls(
        self,
        page: int = 1,
        page_size: int = 50,
        domain: Optional[str] = None,
        search: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Lấy danh sách các URL bị lỗi kèm phân trang, tìm kiếm và lọc."""
        offset = (page - 1) * page_size
        where_clauses = ["status != 'SUCCESS'"]
        params = []

        if domain:
            where_clauses.append("domain = ?")
            params.append(domain)
        if search:
            where_clauses.append("(url LIKE ? OR error LIKE ? OR CAST(id AS TEXT) LIKE ?)")
            term = f"%{search}%"
            params.extend([term, term, term])

        where_sql = " AND ".join(where_clauses)

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM crawl_checkpoint WHERE {where_sql};", params)
            total = cursor.fetchone()[0]

            cursor.execute(f"""
                SELECT id, url, domain, group_id, status, error, crawled_at
                FROM crawl_checkpoint
                WHERE {where_sql}
                ORDER BY crawled_at DESC
                LIMIT ? OFFSET ?;
            """, params + [page_size, offset])
            
            rows = cursor.fetchall()
            results = [
                {
                    "id": r["id"],
                    "url": r["url"],
                    "domain": r["domain"],
                    "group_id": r["group_id"],
                    "status": r["status"],
                    "error": r["error"],
                    "crawled_at": r["crawled_at"]
                }
                for r in rows
            ]

        return results, total

    def retry_urls(self, ids: List[int]) -> int:
        """Xóa các ID khỏi checkpoint để cho phép cào lại."""
        if not ids:
            return 0

        placeholders = ",".join("?" for _ in ids)
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM crawl_checkpoint WHERE id IN ({placeholders});", ids)
            deleted = cursor.rowcount

        for i in ids:
            self._visited_ids.discard(i)

        return deleted

    def retry_all_failed(self, domain: Optional[str] = None) -> int:
        """Xóa toàn bộ các URL bị lỗi khỏi checkpoint để cào lại hàng loạt."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if domain:
                cursor.execute("SELECT id FROM crawl_checkpoint WHERE status != 'SUCCESS' AND domain = ?;", [domain])
                ids_to_remove = [r[0] for r in cursor.fetchall()]
                cursor.execute("DELETE FROM crawl_checkpoint WHERE status != 'SUCCESS' AND domain = ?;", [domain])
            else:
                cursor.execute("SELECT id FROM crawl_checkpoint WHERE status != 'SUCCESS';")
                ids_to_remove = [r[0] for r in cursor.fetchall()]
                cursor.execute("DELETE FROM crawl_checkpoint WHERE status != 'SUCCESS';")
            deleted = cursor.rowcount

        for i in ids_to_remove:
            self._visited_ids.discard(i)

        return deleted

    def get_crawled_documents(
        self,
        page: int = 1,
        page_size: int = 30,
        domain: Optional[str] = None,
        search: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Lấy danh sách các tài liệu cào thành công để hiển thị trong Inspector."""
        offset = (page - 1) * page_size
        where_clauses = ["status = 'SUCCESS'"]
        params = []

        if domain:
            where_clauses.append("domain = ?")
            params.append(domain)
        if search:
            where_clauses.append("(url LIKE ? OR CAST(id AS TEXT) LIKE ?)")
            term = f"%{search}%"
            params.extend([term, term])

        where_sql = " AND ".join(where_clauses)

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM crawl_checkpoint WHERE {where_sql};", params)
            total = cursor.fetchone()[0]

            cursor.execute(f"""
                SELECT id, url, domain, group_id, char_count, crawled_at
                FROM crawl_checkpoint
                WHERE {where_sql}
                ORDER BY crawled_at DESC
                LIMIT ? OFFSET ?;
            """, params + [page_size, offset])
            
            rows = cursor.fetchall()
            results = [
                {
                    "id": r["id"],
                    "url": r["url"],
                    "domain": r["domain"],
                    "group_id": r["group_id"],
                    "char_count": r["char_count"],
                    "crawled_at": r["crawled_at"]
                }
                for r in rows
            ]

        return results, total

    def get_domain_progress(self) -> Dict[str, Dict[str, Any]]:
        """Lấy tiến độ chi tiết của tất cả domain trong database."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    domain,
                    group_id,
                    COUNT(*) as total_crawled,
                    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
                    SUM(CASE WHEN status != 'SUCCESS' THEN 1 ELSE 0 END) as failed_count,
                    AVG(CASE WHEN status = 'SUCCESS' THEN char_count ELSE NULL END) as avg_chars
                FROM crawl_checkpoint
                GROUP BY domain;
            """)
            rows = cursor.fetchall()

        stats = {}
        for r in rows:
            stats[r["domain"]] = {
                "group_id": r["group_id"],
                "total_crawled": r["total_crawled"],
                "success_count": r["success_count"] or 0,
                "failed_count": r["failed_count"] or 0,
                "avg_chars": int(r["avg_chars"] or 0)
            }
        return stats
