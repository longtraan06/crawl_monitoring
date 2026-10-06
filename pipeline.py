"""
Production orchestrator pipeline for large-scale biomedical corpus crawling.
Coordinates Parquet streaming, async worker pools, hybrid extraction,
DocumentStore persistence (per doc_id folder), and real-time state control (Start, Pause, Resume, Stop).
"""

import os
import sys
import time
import asyncio
import logging
from urllib.parse import urlparse
from typing import Optional, List, Set, Dict, Any
import pyarrow.parquet as pq

from .config import (
    CORPUS_PARQUET_PATH,
    DEFAULT_CONCURRENCY,
    MAX_QUEUE_SIZE,
    MIN_CONTENT_CHARS,
    BUFFER_FLUSH_INTERVAL
)
from .domains import is_domain_allowed, get_domain_group
from .models import CrawlTask, CrawlResult
from .fetcher import AsyncFetcher
from .extractor import HybridCleaner
from .storage import CheckpointTracker, DocumentStore

logger = logging.getLogger("vibio_crawler.pipeline")

class CrawlerPipeline:
    """Hệ thống điều phối luồng thu thập dữ liệu bất đồng bộ với đầy đủ cơ chế Pause/Resume/Stop."""

    def __init__(
        self,
        concurrency: int = DEFAULT_CONCURRENCY,
        target_groups: Optional[List[int]] = None,
        target_domains: Optional[Set[str]] = None,
        limit: Optional[int] = None
    ):
        self.concurrency = concurrency
        self.target_groups = target_groups or [1]
        self.target_domains = target_domains
        self.limit = limit
        
        self.task_queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self.result_queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE * 2)
        
        self.checkpoint = CheckpointTracker()
        self.doc_store = DocumentStore()
        self.fetcher = AsyncFetcher()
        self.extractor = HybridCleaner()
        
        # State Machine: IDLE, RUNNING, PAUSED, STOPPED
        self.state = "IDLE"
        self._pause_event = asyncio.Event()
        self._pause_event.set()  # Mặc định không bị pause
        self._stop_requested = False
        
        # Metrics & Telemetry
        self._total_enqueued = 0
        self._total_processed = 0
        self._total_success = 0
        self._total_failed = 0
        self._start_time = 0.0
        self._recent_speeds: List[float] = []

    def pause(self):
        """Tạm dừng nhận và xử lý tác vụ mới (giữ nguyên session và queues)."""
        if self.state == "RUNNING":
            self.state = "PAUSED"
            self._pause_event.clear()
            logger.info("Pipeline đã chuyển sang trạng thái PAUSED.")

    def resume(self):
        """Tiếp tục xử lý tác vụ sau khi tạm dừng."""
        if self.state == "PAUSED":
            self.state = "RUNNING"
            self._pause_event.set()
            logger.info("Pipeline đã RESUME tiếp tục chạy.")

    def stop(self):
        """Dừng hoàn toàn tiến trình cào."""
        self._stop_requested = True
        self.state = "STOPPED"
        self._pause_event.set()  # Mở khóa để các worker đang chờ có thể thoát
        logger.info("Pipeline nhận tín hiệu STOPPED.")

    def get_metrics(self) -> Dict[str, Any]:
        """Lấy các chỉ số đo lường hiệu năng thời gian thực."""
        now = time.time()
        elapsed = now - self._start_time if self._start_time > 0 else 0
        avg_speed_sec = (self._total_processed / elapsed) if elapsed > 0 else 0.0
        speed_per_min = avg_speed_sec * 60.0

        success_rate = (self._total_success / self._total_processed * 100) if self._total_processed > 0 else 0.0

        eta_seconds = None
        if self.limit and avg_speed_sec > 0:
            remaining = max(0, self.limit - self._total_processed)
            eta_seconds = remaining / avg_speed_sec

        return {
            "state": self.state,
            "elapsed_seconds": round(elapsed, 1),
            "speed_doc_per_sec": round(avg_speed_sec, 2),
            "speed_urls_per_min": round(speed_per_min, 1),
            "total_enqueued": self._total_enqueued,
            "total_processed": self._total_processed,
            "total_success": self._total_success,
            "total_failed": self._total_failed,
            "success_rate": round(success_rate, 2),
            "queue_size": self.task_queue.qsize(),
            "target_groups": self.target_groups,
            "concurrency": self.concurrency,
            "limit": self.limit,
            "eta_seconds": round(eta_seconds, 1) if eta_seconds is not None else None
        }

    async def _producer_task(self):
        """Đọc streaming file parquet, lọc domain/nhóm hợp lệ và đẩy vào hàng đợi tác vụ."""
        if not CORPUS_PARQUET_PATH.exists():
            logger.error(f"Không tìm thấy file parquet tại {CORPUS_PARQUET_PATH}")
            for _ in range(self.concurrency):
                await self.task_queue.put(None)
            return

        parquet_file = pq.ParquetFile(str(CORPUS_PARQUET_PATH))
        enqueued_count = 0

        for batch in parquet_file.iter_batches(batch_size=10000, columns=["id", "url"]):
            if self._stop_requested:
                break
                
            ids = batch["id"].to_pylist()
            urls = batch["url"].to_pylist()

            for doc_id, url in zip(ids, urls):
                if self._stop_requested:
                    break

                # 1. Trích xuất domain
                try:
                    domain = urlparse(url).netloc.lower()
                except Exception:
                    continue

                # 2. Kiểm tra whitelist nhóm hoặc danh sách domain chỉ định
                if not is_domain_allowed(domain, self.target_groups, self.target_domains):
                    continue

                # 3. Kiểm tra xem đã crawl trước đó chưa qua cache O(1)
                if self.checkpoint.is_visited(doc_id):
                    continue

                # 4. Đẩy vào hàng đợi
                group_id = get_domain_group(domain)
                task = CrawlTask(id=doc_id, url=url, domain=domain, group_id=group_id)
                await self.task_queue.put(task)
                enqueued_count += 1
                self._total_enqueued = enqueued_count

                if self.limit and enqueued_count >= self.limit:
                    self._stop_requested = True
                    break

        # Đẩy token None báo hiệu kết thúc cho tất cả worker
        for _ in range(self.concurrency):
            await self.task_queue.put(None)

    async def _worker_task(self, worker_id: int):
        """Worker tiêu thụ tác vụ: Fetch -> Extract -> Lưu file document folder -> Result."""
        while not self._stop_requested:
            try:
                # Chờ nếu đang ở trạng thái PAUSED
                await self._pause_event.wait()
                if self._stop_requested:
                    break

                task: Optional[CrawlTask] = await self.task_queue.get()
                if task is None:
                    self.task_queue.task_done()
                    break

                # 1. Tải HTML qua AsyncFetcher
                status_code, html, error = await self.fetcher.fetch(task.url, task.domain)

                # 2. Xử lý kết quả & Trích xuất
                if status_code == 200 and html:
                    title, markdown, char_count = self.extractor.extract(html, task.domain)
                    if char_count >= MIN_CONTENT_CHARS:
                        result = CrawlResult(
                            id=task.id,
                            url=task.url,
                            domain=task.domain,
                            group_id=task.group_id,
                            status="SUCCESS",
                            title=title,
                            markdown=markdown,
                            char_count=char_count
                        )
                        # Lưu trực tiếp vào thư mục documents/{doc_id}/
                        self.doc_store.save_document(result)
                    else:
                        result = CrawlResult(
                            id=task.id,
                            url=task.url,
                            domain=task.domain,
                            group_id=task.group_id,
                            status="EMPTY",
                            title=title,
                            char_count=char_count,
                            error="Nội dung quá ngắn (< 60 ký tự)"
                        )
                else:
                    result = CrawlResult(
                        id=task.id,
                        url=task.url,
                        domain=task.domain,
                        group_id=task.group_id,
                        status="FAILED",
                        error=error or f"Mã lỗi HTTP {status_code}"
                    )

                # 3. Đưa kết quả vào hàng đợi lưu trữ checkpoint
                await self.result_queue.put(result)
                self.task_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} exception: {e}")
                self.task_queue.task_done()

    async def _storage_task(self):
        """Ghi nhận định kỳ vào Checkpoint DB."""
        batch_results: List[CrawlResult] = []
        last_flush_time = time.time()

        while True:
            try:
                result: Optional[CrawlResult] = await self.result_queue.get()
                if result is None:
                    self.result_queue.task_done()
                    break

                batch_results.append(result)
                self._total_processed += 1
                if result.status == "SUCCESS":
                    self._total_success += 1
                else:
                    self._total_failed += 1

                self.result_queue.task_done()

                # Điều kiện flush định kỳ
                now = time.time()
                if len(batch_results) >= 500 or (now - last_flush_time >= BUFFER_FLUSH_INTERVAL):
                    self.checkpoint.record_batch(batch_results)
                    batch_results.clear()
                    last_flush_time = now

            except asyncio.CancelledError:
                break

        # Xả nốt toàn bộ dữ liệu còn lại
        if batch_results:
            self.checkpoint.record_batch(batch_results)
            batch_results.clear()

    async def run(self):
        """Khởi động toàn bộ pipeline điều phối đa nhiệm."""
        self._start_time = time.time()
        self.state = "RUNNING"
        self._stop_requested = False
        self._pause_event.set()

        logger.info(f"Pipeline RUNNING: Concurrency={self.concurrency}, Groups={self.target_groups}")

        # Khởi tạo fetcher session
        await self.fetcher.start()

        storage_coro = asyncio.create_task(self._storage_task())
        worker_coros = [asyncio.create_task(self._worker_task(i)) for i in range(self.concurrency)]
        producer_coro = asyncio.create_task(self._producer_task())

        try:
            await producer_coro
            await asyncio.gather(*worker_coros)
            
            await self.result_queue.put(None)
            await storage_coro

        except (asyncio.CancelledError, KeyboardInterrupt):
            self.stop()
            producer_coro.cancel()
            for w in worker_coros:
                w.cancel()
            await self.result_queue.put(None)
            await storage_coro

        finally:
            await self.fetcher.close()
            self.state = "STOPPED"
            logger.info("Pipeline đã kết thúc phiên làm việc.")
