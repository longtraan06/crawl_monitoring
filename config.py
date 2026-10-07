import os
import json
import logging
import tempfile
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger("vibio_crawler.config")

# Base Paths
PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parent

DATA_DIR = PACKAGE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
LINKS_CORPUS_JSON_PATH = DATA_DIR / "links_corpus.json"
QUERY_JSON_PATH = DATA_DIR / "query.json"

CORPUS_PARQUET_PATH = (
    DATA_DIR / "links_corpus.parquet"
    if (DATA_DIR / "links_corpus.parquet").exists()
    else PROJECT_ROOT / "links_corpus.parquet"
)
OUTPUT_DIR = Path(os.getenv("CRAWLER_OUTPUT_DIR", str(PROJECT_ROOT / "crawler_output")))
DOCUMENTS_DIR = OUTPUT_DIR / "documents"
SHARDS_DIR = OUTPUT_DIR / "shards"

# Custom & RMU JSON Dataset Paths
RMU_JSON_PATH = PACKAGE_DIR / "rmu.json"
RMU_PARQUET_PATH = PACKAGE_DIR / "rmu.parquet"
RMU_TOTAL_URLS = 1464927

CUSTOM_DATASET_CONFIG_PATH = DATA_DIR / "custom_json_config.json"
CUSTOM_PARQUET_PATH = DATA_DIR / "custom_dataset.parquet"
CUSTOM_MANIFEST_PATH = DATA_DIR / "custom_manifest.json"

def get_custom_json_config() -> Optional[dict]:
    """Đọc cấu hình dataset JSON tùy chỉnh nếu đã được thiết lập."""
    if CUSTOM_DATASET_CONFIG_PATH.exists():
        try:
            with open(CUSTOM_DATASET_CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Lỗi đọc custom_json_config.json: {e}")
    return None

def reset_custom_json_dataset() -> bool:
    """Xóa cấu hình dataset hiện tại để người dùng có thể chọn và thiết lập file mới."""
    try:
        if CUSTOM_DATASET_CONFIG_PATH.exists():
            CUSTOM_DATASET_CONFIG_PATH.unlink()
        return True
    except Exception as e:
        logger.error(f"Lỗi reset custom json config: {e}")
        return False

def setup_custom_json_dataset(json_file_path: str) -> dict:
    """
    Tiền xử lý file JSON tùy chỉnh được chỉ định (One-time setup):
    1. Kiểm tra tồn tại và đọc nội dung JSON.
    2. Xác thực cấu trúc: articles kèm id, url, url_host/domain.
    3. Xen kẽ các domain theo Round-Robin (tránh nghẽn server đơn lẻ).
    4. Biên dịch sang custom_dataset.parquet (đọc streaming, không tốn RAM).
    5. Tạo custom_manifest.json phục vụ thống kê tức thì.
    6. Lưu cấu hình vào custom_json_config.json để tự động sử dụng lại.
    """
    p = Path(json_file_path).resolve()
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"Không tìm thấy file tại đường dẫn: {p}")

    from datetime import datetime
    from collections import defaultdict, deque, Counter
    import pyarrow as pa
    import pyarrow.parquet as pq

    logger.info(f"Đang phân tích và xử lý file JSON: {p}")
    with open(p, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict) or "articles" not in data:
        raise ValueError("File JSON không hợp lệ: Cần chứa key cấp gốc 'articles' chứa danh sách bài viết.")

    articles = data.get("articles", [])
    if not articles:
        raise ValueError("Danh sách 'articles' trong file JSON trống.")

    # Gom nhóm theo host/domain
    host_deques = defaultdict(deque)
    for a in articles:
        h = a.get("url_host") or urlparse(a.get("url", "")).netloc or "unknown"
        h = h.lower().strip()
        host_deques[h].append(a)

    # Xen kẽ xoay vòng Round-Robin giữa các domain
    interleaved = []
    active_hosts = list(host_deques.keys())
    while active_hosts:
        next_active = []
        for h in active_hosts:
            dq = host_deques[h]
            if dq:
                interleaved.append(dq.popleft())
                if dq:
                    next_active.append(h)
        active_hosts = next_active

    ids = [a["id"] for a in interleaved]
    urls = [a["url"] for a in interleaved]
    domains = [a.get("url_host") or urlparse(a.get("url", "")).netloc for a in interleaved]

    # Lưu Parquet
    table = pa.Table.from_arrays(
        [pa.array(ids, type=pa.int64()), pa.array(urls, type=pa.string()), pa.array(domains, type=pa.string())],
        names=["id", "url", "domain"]
    )
    pq.write_table(table, str(CUSTOM_PARQUET_PATH), compression="snappy")

    # Lưu Manifest
    host_counter = Counter(a.get("url_host") or urlparse(a.get("url", "")).netloc for a in articles)
    hosts_manifest = [{"url_host": h.lower().strip(), "count": c} for h, c in host_counter.most_common()]

    manifest_data = {
        "file_path": str(p),
        "file_name": p.name,
        "total_urls": len(articles),
        "host_count": len(hosts_manifest),
        "hosts": hosts_manifest
    }
    with open(CUSTOM_MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, ensure_ascii=False, indent=2)

    # Lưu One-time Setup Config
    config_data = {
        "is_configured": True,
        "json_path": str(p),
        "file_name": p.name,
        "file_size_mb": round(p.stat().st_size / (1024 * 1024), 2),
        "total_urls": len(articles),
        "host_count": len(hosts_manifest),
        "parquet_path": str(CUSTOM_PARQUET_PATH),
        "manifest_path": str(CUSTOM_MANIFEST_PATH),
        "setup_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    with open(CUSTOM_DATASET_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config_data, f, ensure_ascii=False, indent=2)

    return config_data

def get_active_dataset_parquet() -> Path:
    """Trả về đường dẫn file parquet đang kích hoạt (custom dataset hoặc rmu.parquet)."""
    cfg = get_custom_json_config()
    if cfg and cfg.get("is_configured") and CUSTOM_PARQUET_PATH.exists():
        return CUSTOM_PARQUET_PATH
    if RMU_PARQUET_PATH.exists():
        return RMU_PARQUET_PATH
    return ensure_rmu_parquet()

def get_active_dataset_manifest() -> Path:
    """Trả về đường dẫn file manifest đang kích hoạt."""
    cfg = get_custom_json_config()
    if cfg and cfg.get("is_configured") and CUSTOM_MANIFEST_PATH.exists():
        return CUSTOM_MANIFEST_PATH
    return DATA_DIR / "rmu_manifest.json"

def get_active_total_urls() -> int:
    """Trả về tổng số URLs trong dataset JSON đang kích hoạt."""
    cfg = get_custom_json_config()
    if cfg and cfg.get("is_configured"):
        return cfg.get("total_urls", RMU_TOTAL_URLS)
    return RMU_TOTAL_URLS

def ensure_rmu_parquet() -> Path:
    """Đảm bảo file rmu.parquet tồn tại, xen kẽ domain (Round-Robin) để tối đa hóa tốc độ crawl mạng."""
    if RMU_PARQUET_PATH.exists():
        return RMU_PARQUET_PATH
    if RMU_JSON_PATH.exists():
        from collections import defaultdict, deque
        import pyarrow as pa
        import pyarrow.parquet as pq

        with open(RMU_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        articles = data.get("articles", [])

        host_deques = defaultdict(deque)
        for a in articles:
            h = a.get("url_host", "unknown").lower()
            host_deques[h].append(a)

        interleaved = []
        active_hosts = list(host_deques.keys())
        while active_hosts:
            next_active = []
            for h in active_hosts:
                dq = host_deques[h]
                if dq:
                    interleaved.append(dq.popleft())
                    if dq:
                        next_active.append(h)
            active_hosts = next_active

        ids = [a["id"] for a in interleaved]
        urls = [a["url"] for a in interleaved]
        hosts = [a.get("url_host", "") for a in interleaved]

        table = pa.Table.from_arrays(
            [pa.array(ids, type=pa.int64()), pa.array(urls, type=pa.string()), pa.array(hosts, type=pa.string())],
            names=["id", "url", "domain"]
        )
        pq.write_table(table, str(RMU_PARQUET_PATH), compression="snappy")
    return RMU_PARQUET_PATH
# Checkpoint Database Configuration
# Hỗ trợ chạy an toàn trên ổ NAS (TRUNCATE mode) hoặc chạy siêu tốc trên ổ cục bộ /tmp (kèm sync sang NAS)
USE_LOCAL_CHECKPOINT = os.getenv("USE_LOCAL_CHECKPOINT", "false").lower() in ("1", "true", "yes")
_NAS_CHECKPOINT_PATH = OUTPUT_DIR / "checkpoint.db"

if USE_LOCAL_CHECKPOINT:
    _LOCAL_DB = Path(tempfile.gettempdir()) / "vibio_checkpoint.db" if os.name == "nt" else Path("/tmp/vibio_checkpoint.db")
    CHECKPOINT_DB_PATH = Path(os.getenv("CHECKPOINT_DB_PATH", str(_LOCAL_DB)))
    CHECKPOINT_BACKUP_PATH = _NAS_CHECKPOINT_PATH
else:
    CHECKPOINT_DB_PATH = Path(os.getenv("CHECKPOINT_DB_PATH", str(_NAS_CHECKPOINT_PATH)))
    CHECKPOINT_BACKUP_PATH = None

# Tự động chọn journal mode: Nếu đường dẫn nằm trên NAS/ổ mạng, dùng TRUNCATE để không sinh file .db-shm gây deadlock
_is_network_fs = any(k in str(CHECKPOINT_DB_PATH).lower() for k in ["share", "nas", "mnt", "nfs", "cifs", "smb"])
SQLITE_JOURNAL_MODE = os.getenv(
    "SQLITE_JOURNAL_MODE",
    "TRUNCATE" if _is_network_fs else "WAL"
)

# Staging Spooler Buffer (Tối ưu cho NAS / Network Mount)
# Crawl ghi vào ổ cục bộ /tmp với tốc độ cực cao, sau mỗi 10 URL luồng mover sẽ di chuyển sang thư mục NAS và xóa tmp
USE_STAGING_BUFFER = os.getenv("USE_STAGING", "true").lower() in ("1", "true", "yes")
_DEFAULT_STAGING_DIR = (
    "/tmp/vibio_staging"
    if os.name != "nt"
    else str(Path(tempfile.gettempdir()) / "vibio_staging")
)
STAGING_DIR = Path(os.getenv("STAGING_DIR", _DEFAULT_STAGING_DIR))
STAGING_BATCH_SIZE = int(os.getenv("STAGING_BATCH", "10"))

# Web Dashboard
DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8000"))

# Crawler Concurrency & Multi-Processing (Tối ưu cho máy Local đa nhân)
DEFAULT_CONCURRENCY = int(os.getenv("CONCURRENCY", "120"))
# Dành 2 nhân CPU cho OS & các tác vụ khác, tối đa 12 workers để tránh lag máy
NUM_CPU_WORKERS = int(os.getenv("CPU_WORKERS", str(max(2, min((os.cpu_count() or 4) - 2, 12)))))
REQUEST_TIMEOUT = 12.0  # Rút ngắn timeout từ 18s xuống 12s để không ngâm worker ở các link chết
MAX_RETRIES = 1        # Thử lại 1 lần thay vì 2 để tăng tốc độ cào toàn tập
RETRY_BACKOFF_FACTOR = 1.0

# Content Extraction Quality Thresholds
MIN_CONTENT_CHARS = 60
MAX_QUEUE_SIZE = 2500   # Giữ hàng đợi RAM nhỏ gọn (< 50MB) chống tràn RAM

# Storage & Sharding
SHARD_SIZE = 50000      # Number of documents per JSONL shard file
BUFFER_FLUSH_SIZE = 500 # Flush buffer to disk every 500 documents
BUFFER_FLUSH_INTERVAL = 3.0  # Tần suất flush nhanh hơn (3s) để giải phóng RAM tức thời

# Browser Impersonation
IMPERSONATE_BROWSER = "chrome120"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6,zh;q=0.5",
    "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}
