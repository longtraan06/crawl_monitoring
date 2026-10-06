import os
import tempfile
from pathlib import Path

# Base Paths
PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parent

DATA_DIR = PACKAGE_DIR / "data"
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
CHECKPOINT_DB_PATH = OUTPUT_DIR / "checkpoint.db"

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

# Crawler Concurrency & Multi-Processing
DEFAULT_CONCURRENCY = int(os.getenv("CONCURRENCY", "100"))
NUM_CPU_WORKERS = int(os.getenv("CPU_WORKERS", str(max(2, min(os.cpu_count() or 4, 32)))))
REQUEST_TIMEOUT = 18.0
MAX_RETRIES = 2
RETRY_BACKOFF_FACTOR = 1.5

# Content Extraction Quality Thresholds
MIN_CONTENT_CHARS = 60
MAX_QUEUE_SIZE = 5000

# Storage & Sharding
SHARD_SIZE = 50000  # Number of documents per JSONL shard file
BUFFER_FLUSH_SIZE = 500  # Flush buffer to disk every 500 documents
BUFFER_FLUSH_INTERVAL = 5.0  # Seconds between periodic flushes

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
