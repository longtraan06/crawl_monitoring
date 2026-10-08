"""
Manager singleton coordinating background pipeline execution and Web API requests.
"""

import asyncio
import logging
from typing import Optional, List, Set, Dict, Any

from ..pipeline import CrawlerPipeline
from ..storage import CheckpointTracker, DocumentStore
from ..domains import get_all_domains_metadata
from ..config import (
    get_custom_json_config,
    setup_custom_json_dataset,
    reset_custom_json_dataset
)

logger = logging.getLogger("vibio_crawler.manager")

class CrawlerManager:
    """Singleton điều phối trạng thái và luồng dữ liệu giữa Web UI và Crawler Pipeline."""

    def __init__(self):
        self.checkpoint = CheckpointTracker()
        self.doc_store = DocumentStore()
        self.current_pipeline: Optional[CrawlerPipeline] = None
        self._background_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._use_staging_pref = True

    def get_status(self) -> Dict[str, Any]:
        """Lấy toàn bộ trạng thái hệ thống: pipeline metrics + checkpoint summary + RMU stats."""
        stats = self.checkpoint.get_stats()
        rmu_stats = self.checkpoint.get_rmu_stats()
        
        if self.current_pipeline is not None:
            pipeline_metrics = self.current_pipeline.get_metrics()
        else:
            pipeline_metrics = {
                "state": "IDLE",
                "source": "corpus",
                "source_total_urls": 3638908,
                "already_crawled_count": stats["total_visited"],
                "session_processed": 0,
                "overall_done_count": stats["total_visited"],
                "overall_progress_percent": round((stats["total_visited"] / 3638908 * 100), 2) if 3638908 > 0 else 0.0,
                "elapsed_seconds": 0.0,
                "speed_doc_per_sec": 0.0,
                "speed_urls_per_min": 0.0,
                "total_enqueued": 0,
                "total_processed": 0,
                "total_success": 0,
                "total_failed": 0,
                "success_rate": 0.0,
                "queue_size": 0,
                "target_groups": [1],
                "concurrency": 50,
                "limit": None,
                "eta_seconds": None,
                "use_staging": self._use_staging_pref,
                "staging_batch_size": 10,
                "total_moved_to_nas": 0,
                "staging_pending": 0
            }

        return {
            "pipeline": pipeline_metrics,
            "corpus": {
                "total_visited": stats["total_visited"],
                "total_chars": stats["total_chars"],
                "status_breakdown": stats["status_breakdown"],
                "group_breakdown": stats.get("group_breakdown", {}),
                "top_domains_success": stats["top_domains_success"]
            },
            "rmu": rmu_stats,
            "json_dataset": self.get_json_dataset_status()
        }

    async def start(
        self,
        groups: Optional[List[int]] = None,
        domains: Optional[List[str]] = None,
        limit: Optional[int] = None,
        concurrency: int = 50,
        timeout: Optional[float] = None,
        use_staging: Optional[bool] = None,
        staging_batch_size: Optional[int] = None,
        source: str = "corpus"
    ) -> Dict[str, Any]:
        """Khởi động một phiên crawl mới trong background task."""
        async with self._lock:
            if self.current_pipeline is not None and self.current_pipeline.state in ("RUNNING", "PAUSED"):
                return {"success": False, "message": f"Crawler đang chạy ở trạng thái {self.current_pipeline.state}."}

            target_groups = groups if groups else [1]
            target_domains_set = set(d.lower().strip() for d in domains) if domains else None
            source_clean = source.lower().strip() if source else "corpus"

            target_staging = self._use_staging_pref if use_staging is None else use_staging
            self._use_staging_pref = target_staging

            self.current_pipeline = CrawlerPipeline(
                concurrency=concurrency,
                target_groups=target_groups,
                target_domains=target_domains_set,
                limit=limit,
                use_staging=target_staging,
                staging_batch_size=staging_batch_size,
                source=source_clean,
                timeout=timeout
            )

            # Chạy pipeline trong asyncio task ngầm
            self._background_task = asyncio.create_task(self._run_pipeline_wrapper())
            timeout_str = f"{self.current_pipeline.timeout}s"
            if source_clean in ("rmu", "json"):
                if target_domains_set:
                    msg = f"Đã khởi động crawl {len(target_domains_set)} domain được chọn từ File JSON ({concurrency} workers, Timeout: {timeout_str})."
                else:
                    msg = f"Đã khởi động crawl toàn bộ File JSON ({concurrency} workers, Timeout: {timeout_str})."
            else:
                msg = f"Đã khởi động crawl Corpus ({concurrency} workers trên Nhóm {target_groups}, Timeout: {timeout_str})."
            return {
                "success": True,
                "message": msg
            }

    async def recrawl_failed(
        self,
        domains: List[str],
        concurrency: int = 50,
        timeout: Optional[float] = None,
        source: str = "json"
    ) -> Dict[str, Any]:
        """Xóa URL lỗi của các domain chỉ định và khởi động cào lại ngay lập tức."""
        if not domains:
            return {"success": False, "message": "Vui lòng chọn ít nhất 1 domain để cào lại URL lỗi."}

        deleted_count = self.checkpoint.retry_all_failed(domains=domains)
        logger.info(f"Đã giải phóng {deleted_count} URL lỗi cho {len(domains)} domain để cào lại.")

        start_res = await self.start(
            domains=domains,
            concurrency=concurrency,
            timeout=timeout,
            source=source
        )

        if not start_res.get("success"):
            return start_res

        return {
            "success": True,
            "retried_count": deleted_count,
            "domains": domains,
            "message": f"Đã giải phóng {deleted_count} URL lỗi và bắt đầu cào lại {len(domains)} domain (Timeout: {timeout or 12}s)."
        }

    async def _run_pipeline_wrapper(self):
        """Wrapper thực thi pipeline ngầm và giải phóng tài nguyên khi hoàn tất."""
        try:
            if self.current_pipeline:
                await self.current_pipeline.run()
        except Exception as e:
            logger.error(f"Lỗi ngoại lệ trong pipeline background: {e}")
        finally:
            if self.current_pipeline:
                self.current_pipeline.state = "STOPPED"

    def pause(self) -> Dict[str, Any]:
        """Tạm dừng pipeline."""
        if not self.current_pipeline or self.current_pipeline.state != "RUNNING":
            return {"success": False, "message": "Pipeline không ở trạng thái RUNNING để pause."}
        self.current_pipeline.pause()
        return {"success": True, "message": "Đã tạm dừng (PAUSED)."}

    def resume(self) -> Dict[str, Any]:
        """Tiếp tục pipeline sau khi pause."""
        if not self.current_pipeline or self.current_pipeline.state != "PAUSED":
            return {"success": False, "message": "Pipeline không ở trạng thái PAUSED để resume."}
        self.current_pipeline.resume()
        return {"success": True, "message": "Đã tiếp tục chạy (RESUMED)."}

    def stop(self) -> Dict[str, Any]:
        """Dừng hoàn toàn pipeline."""
        if not self.current_pipeline or self.current_pipeline.state == "STOPPED":
            return {"success": False, "message": "Pipeline đã dừng hoặc chưa chạy."}
        self.current_pipeline.stop()
        if self._background_task and not self._background_task.done():
            self._background_task.cancel()
        return {"success": True, "message": "Đã gửi tín hiệu dừng (STOPPED)."}

    def toggle_staging(self, enabled: bool) -> Dict[str, Any]:
        """Bật hoặc tắt chế độ Staging Spooler trong thời gian thực hoặc lưu tùy chọn."""
        self._use_staging_pref = enabled
        if self.current_pipeline and self.current_pipeline.state in ("RUNNING", "PAUSED"):
            self.current_pipeline.set_staging(enabled)
            return {
                "success": True,
                "use_staging": enabled,
                "message": f"Đã chuyển Staging Spooler sang {'BẬT (đệm /tmp)' if enabled else 'TẮT (ghi thẳng NAS)'}."
            }
        return {
            "success": True,
            "use_staging": enabled,
            "message": f"Đã lưu cài đặt Staging Spooler là {'BẬT' if enabled else 'TẮT'}."
        }

    def get_domains_data(self) -> Dict[str, Any]:
        """Kết hợp metadata của 97 domain với tiến độ thực tế đã crawl."""
        all_meta = get_all_domains_metadata()
        db_progress = self.checkpoint.get_domain_progress()

        groups = {1: [], 2: [], 3: [], 0: []}

        for item in all_meta:
            d = item["domain"]
            prog = db_progress.get(d, {
                "total_crawled": 0,
                "success_count": 0,
                "failed_count": 0,
                "avg_chars": 0
            })

            url_count = item["url_count"]
            crawled = prog["total_crawled"]
            success = prog["success_count"]
            pct = round((crawled / url_count * 100), 2) if url_count > 0 else 0.0

            domain_data = {
                "domain": d,
                "website_url": item["website_url"],
                "url_count": url_count,
                "percentage_in_corpus": item["percentage"],
                "group_id": item["group_id"],
                "group_name": item["group_name"],
                "note": item["note"],
                "crawled_count": crawled,
                "success_count": success,
                "failed_count": prog["failed_count"],
                "progress_percent": pct,
                "avg_chars": prog["avg_chars"]
            }

            gid = item["group_id"]
            if gid in groups:
                groups[gid].append(domain_data)
            else:
                groups[0].append(domain_data)

        return {
            "group_1": groups[1],
            "group_2": groups[2],
            "group_3": groups[3],
            "other": groups[0]
        }

    def get_failed_urls(
        self,
        page: int = 1,
        page_size: int = 50,
        domain: Optional[str] = None,
        search: Optional[str] = None
    ) -> Dict[str, Any]:
        """Truy vấn danh sách URL lỗi kèm thông tin phân trang."""
        items, total = self.checkpoint.get_failed_urls(page, page_size, domain, search)
        total_pages = (total + page_size - 1) // page_size if total > 0 else 1
        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages
        }

    def retry_urls(self, ids: List[int]) -> Dict[str, Any]:
        """Cào lại danh sách các ID được chọn."""
        deleted = self.checkpoint.retry_urls(ids)
        return {"success": True, "retried_count": deleted}

    def retry_all_failed(self, domain: Optional[str] = None) -> Dict[str, Any]:
        """Cào lại toàn bộ các URL bị lỗi (tùy chọn theo domain)."""
        deleted = self.checkpoint.retry_all_failed(domain)
        return {"success": True, "retried_count": deleted}

    def get_documents(
        self,
        page: int = 1,
        page_size: int = 20,
        domain: Optional[str] = None,
        search: Optional[str] = None
    ) -> Dict[str, Any]:
        """Lấy danh sách tài liệu đã cào để duyệt trong Document Inspector."""
        items, total = self.checkpoint.get_crawled_documents(page, page_size, domain, search)
        total_pages = (total + page_size - 1) // page_size if total > 0 else 1
        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages
        }

    def get_document_detail(self, doc_id: int) -> Optional[Dict[str, Any]]:
        """Lấy chi tiết bài viết (Markdown + metadata) theo ID."""
        return self.doc_store.get_document(doc_id)

    def get_rmu_data(self) -> Dict[str, Any]:
        """Lấy thống kê chi tiết tiến độ RMU Dataset (1.46M URLs) theo từng domain và tổng thể."""
        return self.checkpoint.get_rmu_stats()

    def get_json_dataset_status(self) -> Dict[str, Any]:
        """Lấy trạng thái thiết lập của dataset JSON tùy chỉnh."""
        cfg = get_custom_json_config()
        is_configured = cfg is not None and cfg.get("is_configured", False)
        stats = self.checkpoint.get_rmu_stats()
        return {
            "is_configured": is_configured,
            "config": cfg,
            "stats": stats
        }

    def setup_json_dataset(self, json_path: str) -> Dict[str, Any]:
        """Tiền xử lý và thiết lập dataset JSON mới (one-time setup)."""
        try:
            cfg = setup_custom_json_dataset(json_path)
            stats = self.checkpoint.get_rmu_stats()
            return {
                "success": True,
                "message": f"Đã thiết lập thành công dataset '{cfg['file_name']}' ({cfg['total_urls']:,} URLs, {cfg['host_count']} hosts).",
                "config": cfg,
                "stats": stats
            }
        except Exception as e:
            logger.error(f"Lỗi setup json dataset: {e}")
            return {
                "success": False,
                "message": str(e)
            }

    def reset_json_dataset(self) -> Dict[str, Any]:
        """Hủy cấu hình dataset hiện tại để người dùng có thể chọn file khác."""
        reset_custom_json_dataset()
        return {"success": True, "message": "Đã reset cấu hình dataset."}

# Singleton Instance
crawler_manager = CrawlerManager()
