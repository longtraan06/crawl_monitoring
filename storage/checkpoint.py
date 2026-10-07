"""
High-performance SQLite checkpoint tracker with WAL mode and in-memory cache.
Manages status, errors, and indexing for seamless resume and Web Dashboard analytics.
"""

import json
import shutil
import sqlite3
import logging
from pathlib import Path
from typing import Set, List, Dict, Any, Optional, Tuple

from ..models import CrawlResult
from ..config import (
    CHECKPOINT_DB_PATH,
    CHECKPOINT_BACKUP_PATH,
    SQLITE_JOURNAL_MODE,
    DATA_DIR,
    RMU_TOTAL_URLS,
    get_active_dataset_manifest,
    get_custom_json_config,
    get_active_total_urls
)

logger = logging.getLogger("vibio_crawler.checkpoint")

class CheckpointTracker:
    """Quản lý trạng thái và checkpoint lưu vết quá trình crawl, chống deadlock trên NAS."""

    def __init__(
        self,
        db_path: Path = CHECKPOINT_DB_PATH,
        backup_path: Optional[Path] = CHECKPOINT_BACKUP_PATH
    ):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.backup_path = Path(backup_path) if backup_path else None
        self._visited_ids: Set[int] = set()
        
        # Nếu dùng local DB (/tmp) và trên NAS đã có backup, nạp bản backup về local
        self._sync_from_backup_if_needed()
        self._init_db()
        self._load_cache()

    def _sync_from_backup_if_needed(self):
        """Sao chép database từ NAS về ổ cục bộ /tmp khi khởi động."""
        if self.backup_path and self.backup_path.exists() and not self.db_path.exists():
            try:
                logger.info(f"Đang đồng bộ Checkpoint DB từ NAS ({self.backup_path}) về cục bộ ({self.db_path})...")
                shutil.copy2(str(self.backup_path), str(self.db_path))
            except Exception as e:
                logger.warning(f"Không thể sao chép backup từ NAS về cục bộ: {e}")

    def sync_to_backup(self):
        """Sao lưu online database sang NAS an toàn 100% bằng SQLite Backup API."""
        if self.backup_path and self.db_path.exists():
            try:
                self.backup_path.parent.mkdir(parents=True, exist_ok=True)
                with self._get_connection() as src_conn:
                    with sqlite3.connect(str(self.backup_path), timeout=60.0) as dst_conn:
                        src_conn.backup(dst_conn)
                logger.info(f"Đã sao lưu Checkpoint DB sang NAS ({self.backup_path}).")
            except Exception as e:
                logger.error(f"Lỗi khi sao lưu Checkpoint DB sang NAS: {e}")

    def _get_connection(self) -> sqlite3.Connection:
        """Tạo kết nối SQLite chống treo I/O với busy_timeout và journal mode an toàn."""
        conn = sqlite3.connect(str(self.db_path), timeout=60.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 60000;")  # Chờ 60s thay vì treo kernel vô tận
        
        # Áp dụng journal mode (TRUNCATE cho ổ mạng NAS để không sinh file .db-shm gây deadlock)
        try:
            conn.execute(f"PRAGMA journal_mode={SQLITE_JOURNAL_MODE};")
        except Exception as e:
            logger.warning(f"Lỗi áp dụng journal_mode={SQLITE_JOURNAL_MODE}, fallback sang TRUNCATE: {e}")
            conn.execute("PRAGMA journal_mode=TRUNCATE;")

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
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id FROM crawl_checkpoint;")
                rows = cursor.fetchall()
                self._visited_ids = {row[0] for row in rows}
            logger.info(f"Đã nạp {len(self._visited_ids):,} URLs đã crawl từ checkpoint cache.")
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() or "busy" in str(e).lower():
                logger.error(
                    f"CẢNH BÁO: Database bị khóa (Locked/Busy): {e}. "
                    f"Nếu đang chạy trên ổ NAS, vui lòng chạy lệnh: rm -f '{self.db_path}-shm' '{self.db_path}-wal'"
                )
            raise

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

        # Đồng bộ sang NAS nếu sử dụng chế độ local checkpoint
        if self.backup_path:
            self.sync_to_backup()

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

    def get_rmu_stats(self) -> Dict[str, Any]:
        """Tổng hợp thống kê tiến độ riêng biệt cho tập JSON Dataset (Custom hoặc RMU)."""
        manifest_path = get_active_dataset_manifest()
        hosts_info = []
        total_urls = get_active_total_urls()
        file_name = "rmu.json"
        json_path = ""
        is_configured = False

        cfg = get_custom_json_config()
        if cfg and cfg.get("is_configured"):
            is_configured = True
            file_name = cfg.get("file_name", file_name)
            json_path = cfg.get("json_path", "")
            total_urls = cfg.get("total_urls", total_urls)
        elif manifest_path.exists():
            is_configured = True

        if manifest_path.exists():
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest = json.load(f)
                    hosts_info = manifest.get("hosts", [])
                    total_urls = manifest.get("total_urls", total_urls)
                    if not json_path:
                        json_path = manifest.get("file_path", "")
                    if not file_name:
                        file_name = manifest.get("file_name", "dataset.json")
            except Exception as e:
                logger.error(f"Lỗi đọc manifest {manifest_path}: {e}")

        # Lấy danh sách hosts để truy vấn
        hosts_map = {h["url_host"].lower(): h["count"] for h in hosts_info}
        domain_progress = {}

        if hosts_map:
            # Chuẩn bị danh sách query bao gồm cả domain có hoặc không có 'www.'
            query_domains = set()
            domain_to_host = {}
            for h_domain in hosts_map.keys():
                query_domains.add(h_domain)
                domain_to_host[h_domain] = h_domain
                if h_domain.startswith("www."):
                    alt = h_domain[4:]
                    query_domains.add(alt)
                    domain_to_host[alt] = h_domain
                else:
                    alt = f"www.{h_domain}"
                    query_domains.add(alt)
                    domain_to_host[alt] = h_domain

            with self._get_connection() as conn:
                cursor = conn.cursor()
                placeholders = ",".join(["?"] * len(query_domains))
                cursor.execute(f"""
                    SELECT domain, status, count(*), coalesce(sum(char_count), 0)
                    FROM crawl_checkpoint
                    WHERE domain IN ({placeholders})
                    GROUP BY domain, status;
                """, list(query_domains))
                for d, status, count, chars in cursor.fetchall():
                    d_clean = d.lower()
                    target_host = domain_to_host.get(d_clean, d_clean)
                    if target_host not in domain_progress:
                        domain_progress[target_host] = {"crawled": 0, "success": 0, "failed": 0, "chars": 0}
                    domain_progress[target_host]["crawled"] += count
                    domain_progress[target_host]["chars"] += chars
                    if status == "SUCCESS":
                        domain_progress[target_host]["success"] += count
                    else:
                        domain_progress[target_host]["failed"] += count

        total_crawled = 0
        total_success = 0
        total_failed = 0
        total_chars = 0
        hosts_result = []

        for h in hosts_info:
            d = h["url_host"].lower()
            url_count = h["count"]
            prog = domain_progress.get(d, {"crawled": 0, "success": 0, "failed": 0, "chars": 0})

            crawled = prog["crawled"]
            success = prog["success"]
            failed = prog["failed"]
            chars = prog["chars"]
            pct = round((crawled / url_count * 100), 2) if url_count > 0 else 0.0

            total_crawled += crawled
            total_success += success
            total_failed += failed
            total_chars += chars

            hosts_result.append({
                "domain": h["url_host"],
                "url_count": url_count,
                "crawled_count": crawled,
                "success_count": success,
                "failed_count": failed,
                "progress_percent": pct,
                "char_count": chars,
                "status": "COMPLETED" if (crawled >= url_count and url_count > 0) else ("IN_PROGRESS" if crawled > 0 else "PENDING")
            })

        hosts_result.sort(key=lambda x: x["url_count"], reverse=True)
        remaining = max(0, total_urls - total_crawled)
        pct_overall = round((total_crawled / total_urls * 100), 2) if total_urls > 0 else 0.0

        return {
            "is_configured": is_configured,
            "file_name": file_name,
            "json_path": json_path,
            "total_urls": total_urls,
            "host_count": len(hosts_info),
            "already_crawled": total_crawled,
            "crawled_success": total_success,
            "crawled_failed": total_failed,
            "remaining_urls": remaining,
            "progress_percent": pct_overall,
            "total_chars": total_chars,
            "hosts": hosts_result
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
