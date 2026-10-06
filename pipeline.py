"""
High-throughput production orchestrator pipeline for large-scale biomedical corpus crawling.
Optimized for multi-core CPUs and high-latency filesystems (like NAS / network mounts):
- Offloads HTML parsing to Thread/Process pool to keep Async Event Loop 100% unblocked.
- Asynchronous Non-blocking Document Writer Queue (decouples network fetching from disk latency).
- High concurrency support (100 - 250+ workers).
- Full state control (Start, Pause, Resume, Stop).
"""

import os
import sys
import time
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
from typing import Optional, List, Set, Dict, Any
import pyarrow.parquet as pq

from .config import (
    CORPUS_PARQUET_PATH,
    DEFAULT_CONCURRENCY,
    NUM_CPU_WORKERS,
    MAX_QUEUE_SIZE,
    MIN_CONTENT_CHARS,
    BUFFER_FLUSH_INTERVAL,
    USE_STAGING_BUFFER,
    STAGING_BATCH_SIZE
)
from .domains import is_domain_allowed, get_domain_group
from .models import CrawlTask, CrawlResult
from .fetcher import AsyncFetcher
from .extractor import HybridCleaner
from .storage import CheckpointTracker, DocumentStore

logger = logging.getLogger("vibio_crawler.pipeline")

def _parse_worker_fn(html_content: str, domain: str):
    """Hàm bóc tách chạy độc lập trên worker thread để giải phóng GIL của Event Loop."""
    return HybridCleaner.extract(html_content, domain)

class CrawlerPipeline:
    """Hệ thống điều phối luồng thu thập dữ liệu bất đồng bộ tối ưu hóa hiệu năng cao."""

    def __init__(
        self,
        concurrency: int = DEFAULT_CONCURRENCY,
        target_groups: Optional[List[int]] = None,
        target_domains: Optional[Set[str]] = None,
        limit: Optional[int] = None,
        use_staging: Optional[bool] = None,
        staging_batch_size: Optional[int] = None,
        include_group_2: bool = False
    ):
        self.concurrency = concurrency
        if include_group_2 and not target_groups:
            self.target_groups = [1, 2]
        else:
            self.target_groups = target_groups or [1]
        self.target_domains = target_domains
        self.limit = limit
        
        self.use_staging = USE_STAGING_BUFFER if use_staging is None else use_staging
        self.staging_batch_size = STAGING_BATCH_SIZE if staging_batch_size is None else staging_batch_size
        
        self.task_queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self.result_queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE * 2)
        self.doc_write_queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE * 2)
        self.staging_mover_queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE * 2)
        
        self.checkpoint = CheckpointTracker()
        self.doc_store = DocumentStore(use_staging=self.use_staging)
        self.fetcher = AsyncFetcher()
        self.extractor = HybridCleaner()
        
        # ThreadPool đa nhân CPU cho HTML parsing
        self.cpu_pool = ThreadPoolExecutor(max_workers=NUM_CPU_WORKERS)
        
        # State Machine: IDLE, RUNNING, PAUSED, STOPPED
        self.state = "IDLE"
        self._pause_event = asyncio.Event()
        self._pause_event.set()
        self._stop_requested = False
        
        # Metrics & Telemetry
        self._total_enqueued = 0
        self._total_processed = 0
        self._total_success = 0
        self._total_failed = 0
        self._total_moved_to_nas = 0
        self._start_time = 0.0

    def pause(self):
        """Tạm dừng nhận và xử lý tác vụ mới."""
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
        self._pause_event.set()
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
            "eta_seconds": round(eta_seconds, 1) if eta_seconds is not None else None,
            "use_staging": self.use_staging,
            "staging_batch_size": self.staging_batch_size,
            "total_moved_to_nas": self._total_moved_to_nas,
            "staging_pending": self.staging_mover_queue.qsize() if self.use_staging else 0
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

                try:
                    domain = urlparse(url).netloc.lower()
                except Exception:
                    continue

                if not is_domain_allowed(domain, self.target_groups, self.target_domains):
                    continue

                if self.checkpoint.is_visited(doc_id):
                    continue

                group_id = get_domain_group(domain)
                task = CrawlTask(id=doc_id, url=url, domain=domain, group_id=group_id)
                await self.task_queue.put(task)
                enqueued_count += 1
                self._total_enqueued = enqueued_count

                if self.limit and enqueued_count >= self.limit:
                    self._stop_requested = True
                    break

        for _ in range(self.concurrency):
            await self.task_queue.put(None)

    async def _worker_task(self, worker_id: int):
        """Worker tiêu thụ tác vụ: Fetch mạng -> CPU Parse -> Đẩy vào Write Queue."""
        loop = asyncio.get_running_loop()

        while not self._stop_requested:
            try:
                await self._pause_event.wait()
                if self._stop_requested:
                    break

                task: Optional[CrawlTask] = await self.task_queue.get()
                if task is None:
                    self.task_queue.task_done()
                    break

                # 1. Tải HTML qua AsyncFetcher (I/O Bound)
                status_code, html, error = await self.fetcher.fetch(task.url, task.domain)

                # 2. Bóc tách HTML song song trên CPU Pool (Không làm khựng Event Loop)
                if status_code == 200 and html:
                    title, markdown, char_count = await loop.run_in_executor(
                        self.cpu_pool, _parse_worker_fn, html, task.domain
                    )
                    
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
                        # Đẩy vào queue ghi đĩa bất đồng bộ (0ms latency, không chờ I/O NAS)
                        await self.doc_write_queue.put(result)
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

    async def _disk_writer_task(self):
        """Tiến trình ghi đĩa ngầm độc lập: Nhận tài liệu từ hàng đợi và lưu vào staging /tmp."""
        loop = asyncio.get_running_loop()
        while True:
            try:
                result: Optional[CrawlResult] = await self.doc_write_queue.get()
                if result is None:
                    self.doc_write_queue.task_done()
                    break

                # Ghi đĩa vào staging_dir (/tmp) với tốc độ cao, không nghẽn bởi NAS
                await loop.run_in_executor(None, self.doc_store.save_document, result)
                
                # Nếu bật Staging: gửi id sang mover queue để gom batch 10 URL chuyển sang NAS
                if self.use_staging:
                    await self.staging_mover_queue.put(result.id)

                self.doc_write_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Lỗi ghi đĩa: {e}")
                self.doc_write_queue.task_done()

    async def _staging_mover_task(self):
        """
        Luồng ngầm di chuyển tài liệu độc lập:
        Cứ sau 10 URLs được crawl vào /tmp, khởi tạo luồng chuyển (mv) về thư mục project trên NAS,
        đồng thời xóa sạch dữ liệu đệm ở /tmp để chống tràn ổ home/root.
        """
        loop = asyncio.get_running_loop()
        batch_ids: List[int] = []
        last_move_time = time.time()

        while True:
            try:
                # Đọc ID từ queue với timeout 2.0s
                try:
                    doc_id = await asyncio.wait_for(
                        self.staging_mover_queue.get(),
                        timeout=2.0
                    )
                except asyncio.TimeoutError:
                    doc_id = "TIMEOUT"

                if doc_id is None:
                    self.staging_mover_queue.task_done()
                    break

                if doc_id != "TIMEOUT":
                    batch_ids.append(doc_id)
                    self.staging_mover_queue.task_done()

                # Điều kiện kích hoạt luồng di chuyển:
                # 1. Đạt mốc 10 URL (staging_batch_size)
                # 2. Hoặc sau 3 giây nếu có ít nhất 1 URL đang chờ
                now = time.time()
                should_move = (len(batch_ids) >= self.staging_batch_size) or (
                    len(batch_ids) > 0 and (now - last_move_time >= 3.0)
                )

                if should_move and batch_ids:
                    to_move = batch_ids.copy()
                    batch_ids.clear()
                    last_move_time = now

                    moved = await loop.run_in_executor(
                        None,
                        self.doc_store.move_batch_to_destination,
                        to_move
                    )
                    self._total_moved_to_nas += moved
                    logger.info(
                        f"[Staging Mover] Đã mv batch {moved} documents từ /tmp sang NAS & dọn dẹp sạch /tmp. "
                        f"(Tổng đã chuyển NAS: {self._total_moved_to_nas})"
                    )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Lỗi trong staging mover task: {e}")

        # Chuyển nốt số URL còn lại trong mẻ cuối khi dừng pipeline
        if batch_ids:
            moved = await loop.run_in_executor(
                None,
                self.doc_store.move_batch_to_destination,
                batch_ids
            )
            self._total_moved_to_nas += moved
            batch_ids.clear()

        # Quét dọn lần cuối toàn bộ staging dir
        await loop.run_in_executor(None, self.doc_store.flush_remaining_staging)

    async def _storage_task(self):
        """Ghi nhận định kỳ vào Checkpoint DB theo transaction batch."""
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

                # Điều kiện flush định kỳ vào SQLite
                now = time.time()
                if len(batch_results) >= 500 or (now - last_flush_time >= BUFFER_FLUSH_INTERVAL):
                    self.checkpoint.record_batch(batch_results)
                    batch_results.clear()
                    last_flush_time = now

            except asyncio.CancelledError:
                break

        if batch_results:
            self.checkpoint.record_batch(batch_results)
            batch_results.clear()

    def set_staging(self, enabled: bool):
        """Bật hoặc tắt chế độ Staging Spooler thời gian thực."""
        self.use_staging = enabled
        self.doc_store.use_staging = enabled
        if not enabled:
            self.doc_store.flush_remaining_staging()
        logger.info(f"Staging Spooler switched to: {'ENABLED' if enabled else 'DISABLED'}")

    async def run(self):
        """Khởi động toàn bộ pipeline điều phối đa nhiệm với tối ưu hóa đa nhân và Staging Mover."""
        self._start_time = time.time()
        self.state = "RUNNING"
        self._stop_requested = False
        self._pause_event.set()

        logger.info(
            f"Pipeline RUNNING: Concurrency={self.concurrency}, CPU Workers={NUM_CPU_WORKERS}, "
            f"Staging Buffer={'BẬT (Mỗi ' + str(self.staging_batch_size) + ' URLs mv sang NAS)' if self.use_staging else 'TẮT'}"
        )

        await self.fetcher.start()

        storage_coro = asyncio.create_task(self._storage_task())
        disk_writer_coro = asyncio.create_task(self._disk_writer_task())
        staging_mover_coro = asyncio.create_task(self._staging_mover_task())
        worker_coros = [asyncio.create_task(self._worker_task(i)) for i in range(self.concurrency)]
        producer_coro = asyncio.create_task(self._producer_task())

        try:
            await producer_coro
            await asyncio.gather(*worker_coros)
            
            # Gửi sentinel kết thúc tuần tự: writer -> mover -> storage
            await self.doc_write_queue.put(None)
            await disk_writer_coro

            await self.staging_mover_queue.put(None)
            await staging_mover_coro

            await self.result_queue.put(None)
            await storage_coro

        except (asyncio.CancelledError, KeyboardInterrupt):
            self.stop()
            producer_coro.cancel()
            for w in worker_coros:
                w.cancel()
            await self.doc_write_queue.put(None)
            await disk_writer_coro
            await self.staging_mover_queue.put(None)
            await staging_mover_coro
            await self.result_queue.put(None)
            await storage_coro

        finally:
            await self.fetcher.close()
            self.cpu_pool.shutdown(wait=False)
            self.doc_store.flush_remaining_staging()
            self.state = "STOPPED"
            logger.info("Pipeline đã kết thúc phiên làm việc.")
