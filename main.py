"""
CLI entry point for ViBioMIR Biomedical Crawler.
"""

import sys
import argparse
import asyncio
import logging

sys.stdout.reconfigure(encoding='utf-8')

from .config import DEFAULT_CONCURRENCY
from .pipeline import CrawlerPipeline
from .storage import CheckpointTracker

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

def show_stats():
    """Hiển thị thống kê tổng quan từ cơ sở dữ liệu checkpoint."""
    tracker = CheckpointTracker()
    stats = tracker.get_stats()
    print("=" * 60)
    print("THỐNG KÊ TIẾN ĐỘ CORPUS HIỆN TẠI")
    print(f"Tổng số URL đã ghé thăm: {stats['total_visited']:,}")
    print(f"Tổng số ký tự text sạch trích xuất: {stats['total_chars']:,}")
    print("\nPhân rã theo trạng thái:")
    for status, count in stats['status_breakdown'].items():
        print(f"  - {status:<15}: {count:,}")
    print("\nTop 10 Domain thành công nhiều nhất:")
    for domain, count in stats['top_domains_success']:
        print(f"  - {domain:<35}: {count:,}")
    print("=" * 60)

def main():
    parser = argparse.ArgumentParser(description="ViBioMIR Production Crawler CLI")
    parser.add_argument(
        "--mode",
        choices=["dashboard", "run", "sample", "stats"],
        default="dashboard",
        help="Chế độ hoạt động: 'dashboard' (mở Web UI), 'run' (cào toàn bộ CLI), 'sample' (chạy mẫu), 'stats' (xem tiến độ)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Cổng chạy Web Dashboard (mặc định: 8000)"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Địa chỉ host chạy Web Dashboard (mặc định: 127.0.0.1)"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=f"Số lượng worker async chạy song song (mặc định: {DEFAULT_CONCURRENCY})"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Giới hạn số lượng URL muốn cào (dùng khi test thử nghiệm hoặc chế độ sample)"
    )
    parser.add_argument(
        "--include-group-2",
        action="store_true",
        help="Bao gồm cả Nhóm 2 (Long Châu, Wujue) bên cạnh 86 domain Nhóm 1"
    )

    args = parser.parse_args()

    if args.mode == "dashboard":
        from .dashboard.server import run_server
        run_server(host=args.host, port=args.port)
        return

    if args.mode == "stats":
        show_stats()
        return

    limit = args.limit
    if args.mode == "sample" and not limit:
        limit = 500

    pipeline = CrawlerPipeline(
        concurrency=args.concurrency,
        include_group_2=args.include_group_2,
        limit=limit
    )

    try:
        asyncio.run(pipeline.run())
    except KeyboardInterrupt:
        print("\nĐã hủy tiến trình.")

if __name__ == "__main__":
    main()
