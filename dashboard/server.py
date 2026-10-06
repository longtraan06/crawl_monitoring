"""
Server entry point to launch the ViBioMIR Web Dashboard.
"""

import sys
import uvicorn

sys.stdout.reconfigure(encoding='utf-8')

from ..config import DASHBOARD_HOST, DASHBOARD_PORT

def run_server(host: str = DASHBOARD_HOST, port: int = DASHBOARD_PORT):
    """Khởi chạy máy chủ Web Monitoring Dashboard."""
    print("=" * 65)
    print("  ViBioMIR CRAWLER ORCHESTRATOR & MONITORING STUDIO")
    print(f"  Truy cập Dashboard tại: http://{host}:{port}")
    print("  Nhấn Ctrl+C để dừng server.")
    print("=" * 65)
    uvicorn.run("vibio_crawler.dashboard.app:app", host=host, port=port, log_level="warning")

if __name__ == "__main__":
    run_server()
