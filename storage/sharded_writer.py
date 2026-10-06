"""
Buffered, sharded JSONL writer for large-scale biomedical document storage.
Splits data into chunked files (e.g. 50,000 docs/shard) for efficient indexing and transfer.
"""

import os
import json
import logging
from pathlib import Path
from typing import List, Optional, TextIO

from ..models import CrawlResult
from ..config import SHARDS_DIR, SHARD_SIZE, BUFFER_FLUSH_SIZE

logger = logging.getLogger("vibio_crawler.storage")

class ShardedWriter:
    """Ghi dữ liệu tài liệu ra các file JSONL phân đoạn an toàn và hiệu năng cao."""

    def __init__(self, shards_dir: Path = SHARDS_DIR, shard_size: int = SHARD_SIZE):
        self.shards_dir = Path(shards_dir)
        self.shards_dir.mkdir(parents=True, exist_ok=True)
        self.shard_size = shard_size
        
        self.current_shard_idx = 1
        self.current_shard_count = 0
        self.current_file_handle: Optional[TextIO] = None
        
        self._buffer: List[dict] = []
        self._init_active_shard()

    def _init_active_shard(self):
        """Xác định shard đang hoạt động gần nhất để ghi nối tiếp."""
        existing_shards = sorted(self.shards_dir.glob("corpus_shard_*.jsonl"))
        if existing_shards:
            latest_shard = existing_shards[-1]
            try:
                # Trích xuất số thứ tự từ tên file
                part = latest_shard.stem.split("_")[-1]
                self.current_shard_idx = int(part)
                # Đếm số dòng hiện có
                with open(latest_shard, "r", encoding="utf-8") as f:
                    self.current_shard_count = sum(1 for _ in f)

                # Nếu shard hiện tại đã đầy, chuyển sang shard kế tiếp
                if self.current_shard_count >= self.shard_size:
                    self.current_shard_idx += 1
                    self.current_shard_count = 0
            except Exception:
                self.current_shard_idx = len(existing_shards) + 1
                self.current_shard_count = 0

        self._open_shard_file()

    def _open_shard_file(self):
        """Mở file shard tương ứng ở chế độ append (a)."""
        if self.current_file_handle and not self.current_file_handle.closed:
            self.current_file_handle.close()

        file_name = f"corpus_shard_{self.current_shard_idx:04d}.jsonl"
        shard_path = self.shards_dir / file_name
        self.current_file_handle = open(shard_path, "a", encoding="utf-8")
        logger.info(f"Đang ghi dữ liệu vào shard: {file_name} (Hiện có: {self.current_shard_count:,} docs)")

    def write(self, result: CrawlResult):
        """Thêm kết quả hợp lệ vào buffer ghi."""
        if not result.is_valid_document():
            return

        self._buffer.append(result.to_record())
        if len(self._buffer) >= BUFFER_FLUSH_SIZE:
            self.flush()

    def flush(self):
        """Xả buffer ra ổ đĩa và kiểm tra điều kiện chuyển shard."""
        if not self._buffer or not self.current_file_handle:
            return

        lines = [json.dumps(record, ensure_ascii=False) + "\n" for record in self._buffer]
        self.current_file_handle.writelines(lines)
        self.current_file_handle.flush()

        self.current_shard_count += len(self._buffer)
        self._buffer.clear()

        # Kiểm tra ngưỡng dung lượng shard để tách file mới
        if self.current_shard_count >= self.shard_size:
            self.current_shard_idx += 1
            self.current_shard_count = 0
            self._open_shard_file()

    def close(self):
        """Xả hết buffer còn lại và đóng file handle."""
        self.flush()
        if self.current_file_handle and not self.current_file_handle.closed:
            self.current_file_handle.close()
            self.current_file_handle = None
