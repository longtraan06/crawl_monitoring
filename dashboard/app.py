"""
FastAPI application for ViBioMIR Web Monitoring and Orchestration Dashboard.
"""

import asyncio
from pathlib import Path
from typing import Optional, List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .manager import crawler_manager
from ..config import DASHBOARD_HOST, DASHBOARD_PORT

app = FastAPI(
    title="ViBioMIR Crawler Orchestrator",
    description="Web Monitoring & Orchestration Dashboard for Biomedical Corpus Crawling",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

STATIC_DIR = Path(__file__).resolve().parent / "static"
STATIC_DIR.mkdir(parents=True, exist_ok=True)

# Mount static folder
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

class StartCrawlRequest(BaseModel):
    groups: Optional[List[int]] = [1]
    domains: Optional[List[str]] = None
    limit: Optional[int] = None
    concurrency: int = 50
    timeout: Optional[float] = None
    use_staging: Optional[bool] = None
    staging_batch_size: Optional[int] = None
    source: Optional[str] = "corpus"

class RecrawlFailedRequest(BaseModel):
    domains: List[str]
    concurrency: Optional[int] = 50
    timeout: Optional[float] = None
    source: Optional[str] = "json"

class RetryRequest(BaseModel):
    ids: List[int]

class ToggleStagingRequest(BaseModel):
    enabled: bool

class SetupJsonRequest(BaseModel):
    json_path: str

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    """Phục vụ file giao diện chính của Dashboard."""
    index_file = STATIC_DIR / "index.html"
    if index_file.exists():
        with open(index_file, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse("<h2>ViBioMIR Dashboard frontend is initializing...</h2>")

# --- CRAWLER CONTROL ENDPOINTS ---

@app.post("/api/crawler/start")
async def start_crawl(req: StartCrawlRequest):
    """Bắt đầu tiến trình crawl theo nhóm hoặc domain chỉ định."""
    res = await crawler_manager.start(
        groups=req.groups,
        domains=req.domains,
        limit=req.limit,
        concurrency=req.concurrency,
        timeout=req.timeout,
        use_staging=req.use_staging,
        staging_batch_size=req.staging_batch_size,
        source=req.source or "corpus"
    )
    return JSONResponse(res)

@app.post("/api/crawler/recrawl-failed")
async def recrawl_failed_urls(req: RecrawlFailedRequest):
    """Xóa các URL lỗi của các domain được chọn và khởi động cào lại ngay lập tức."""
    res = await crawler_manager.recrawl_failed(
        domains=req.domains,
        concurrency=req.concurrency or 50,
        timeout=req.timeout,
        source=req.source or "json"
    )
    return JSONResponse(res)

@app.post("/api/crawler/pause")
async def pause_crawl():
    """Tạm dừng cào (PAUSE)."""
    res = crawler_manager.pause()
    return JSONResponse(res)

@app.post("/api/crawler/resume")
async def resume_crawl():
    """Tiếp tục cào (RESUME)."""
    res = crawler_manager.resume()
    return JSONResponse(res)

@app.post("/api/crawler/stop")
async def stop_crawl():
    """Dừng hoàn toàn tiến trình cào."""
    res = crawler_manager.stop()
    return JSONResponse(res)

@app.post("/api/crawler/staging-toggle")
async def toggle_staging(req: ToggleStagingRequest):
    """Bật/tắt chế độ Staging Spooler thời gian thực."""
    res = crawler_manager.toggle_staging(req.enabled)
    return JSONResponse(res)

@app.get("/api/crawler/status")
async def get_crawler_status():
    """Lấy trạng thái tổng quan của crawler và checkpoint database."""
    return JSONResponse(crawler_manager.get_status())

@app.get("/api/crawler/rmu/stats")
async def get_rmu_stats():
    """Lấy thống kê tiến độ riêng biệt cho tập dữ liệu JSON / RMU (backward compatibility)."""
    return JSONResponse(crawler_manager.get_rmu_data())

@app.get("/api/dataset/json/status")
async def get_json_dataset_status():
    """Lấy trạng thái thiết lập và thống kê của dataset JSON tùy chỉnh."""
    return JSONResponse(crawler_manager.get_json_dataset_status())

@app.post("/api/dataset/json/setup")
async def setup_json_dataset(req: SetupJsonRequest):
    """Tiền xử lý và kích hoạt dataset JSON mới từ đường dẫn được chỉ định (One-time setup)."""
    res = crawler_manager.setup_json_dataset(req.json_path)
    return JSONResponse(res)

@app.post("/api/dataset/json/reset")
async def reset_json_dataset():
    """Hủy cấu hình dataset hiện tại để người dùng có thể đổi sang file JSON khác."""
    res = crawler_manager.reset_json_dataset()
    return JSONResponse(res)

# --- DOMAINS METADATA & PROGRESS ---

@app.get("/api/domains")
async def get_domains():
    """Lấy danh sách 97 domain phân theo 3 nhóm kèm tiến độ crawl thực tế."""
    return JSONResponse(crawler_manager.get_domains_data())

# --- FAILED URLS & RETRY ---

@app.get("/api/failed-urls")
async def get_failed_urls(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    domain: Optional[str] = None,
    search: Optional[str] = None
):
    """Truy vấn danh sách URL bị lỗi kèm lý do kỹ thuật chi tiết."""
    res = crawler_manager.get_failed_urls(page, page_size, domain, search)
    return JSONResponse(res)

@app.post("/api/failed-urls/retry")
async def retry_failed_urls(req: RetryRequest):
    """Xóa các URL lỗi được chỉ định để đưa lại vào hàng đợi cào."""
    res = crawler_manager.retry_urls(req.ids)
    return JSONResponse(res)

@app.post("/api/failed-urls/retry-all")
async def retry_all_failed_urls(domain: Optional[str] = None):
    """Cào lại toàn bộ các URL bị lỗi (hoặc của một domain cụ thể)."""
    res = crawler_manager.retry_all_failed(domain)
    return JSONResponse(res)

# --- DOCUMENTS INSPECTOR & SIDE-BY-SIDE ---

@app.get("/api/documents")
async def get_crawled_documents(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    domain: Optional[str] = None,
    search: Optional[str] = None
):
    """Lấy danh sách các tài liệu đã cào thành công để duyệt."""
    res = crawler_manager.get_documents(page, page_size, domain, search)
    return JSONResponse(res)

@app.get("/api/documents/{doc_id}")
async def get_document_detail(doc_id: int):
    """Lấy chi tiết bài viết (Markdown + Metadata) cho màn hình đối soát Side-by-side."""
    doc = crawler_manager.get_document_detail(doc_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài liệu này trong storage.")
    return JSONResponse(doc)

# --- WEBSOCKET REAL-TIME TELEMETRY ---

@app.websocket("/ws/telemetry")
async def websocket_telemetry(websocket: WebSocket):
    """Kênh WebSocket stream tốc độ và metrics mỗi giây."""
    await websocket.accept()
    try:
        while True:
            status = crawler_manager.get_status()
            await websocket.send_json(status)
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
