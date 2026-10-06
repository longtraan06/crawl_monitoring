# ViBioMIR Production Crawler & Monitoring Studio

Hệ thống crawl, trích xuất dữ liệu bài viết y tế quy mô lớn (~4.39 triệu URLs) và Web Dashboard điều phối, giám sát toàn diện cho Corpus ViBioMIR.

---

## 1. Cấu Trúc Thư Mục Hệ Thống

```text
vibio_crawler/
├── __init__.py               # Khởi tạo package
├── config.py                 # Toàn bộ cấu hình hệ thống (concurrency, timeout, paths, dashboard)
├── domains.py                # Danh mục 97 domain phân theo 3 nhóm (Group 1, 2, 3) & Corpus Metadata
├── models.py                 # Data models chuẩn (CrawlTask, CrawlResult, DocumentRecord)
├── data/                     # Thư mục chứa dữ liệu đầu vào gốc
│   ├── links_corpus.json     # 4.39M links corpus định dạng JSON
│   └── query.json            # 1,200 câu truy vấn y tế tiếng Việt JSON
├── fetcher/                  # [TẦNG 1: MẠNG & KẾT NỐI]
│   ├── __init__.py
│   ├── client.py             # AsyncFetcher: curl_cffi giả lập Chrome 120, cơ chế retry, backoff
│   └── handlers.py           # DomainHandlerRegistry: xử lý cookie D1N (Lao Động), headers riêng
├── extractor/                # [TẦNG 2: TRÍCH XUẤT & BẢO TOÀN HEADING]
│   ├── __init__.py
│   ├── hybrid_cleaner.py     # HybridCleaner: Readability bọc <article> + Trafilatura giữ nguyên H2/H3
│   └── rules_120ask.py       # Bộ trích xuất chuyên sâu cho diễn đàn bác sĩ 120ask.com
├── storage/                  # [TẦNG 3: LƯU TRỮ PHÂN ĐOẠN & CHECKPOINT]
│   ├── __init__.py
│   ├── document_store.py     # DocumentStore: Lưu từng bài vào folder documents/{doc_id}/ (content.md + metadata.json)
│   ├── checkpoint.py         # CheckpointTracker: SQLite WAL mode + RAM cache O(1) chống trùng lặp & quản lý URL lỗi
│   └── sharded_writer.py     # ShardedWriter: Ghi nối tiếp các file JSONL phân đoạn (50k docs/shard)
├── pipeline.py               # [TẦNG 4: ĐIỀU PHỐI ĐA NHIỆM]
│   └── (Producer Parquet Stream -> Queue -> Workers -> DocumentStore -> Checkpoint)
├── dashboard/                # [GIAO DIỆN WEB MONITORING & STUDIO]
│   ├── __init__.py
│   ├── app.py                # FastAPI Application (REST API & WebSocket Telemetry)
│   ├── manager.py            # Singleton CrawlerManager điều phối trạng thái (Start/Pause/Resume/Stop)
│   ├── server.py             # Uvicorn launcher
│   └── static/
│       └── index.html        # Modern Single-Page Dashboard (TailwindCSS, Chart.js, Marked.js)
├── main.py                   # Giao diện dòng lệnh CLI điều khiển chính
└── README.md                 # Tài liệu kỹ thuật
```

---

## 2. Cấu Trúc Dữ Liệu Đầu Ra (Document Store)

Toàn bộ tài liệu crawl thành công được lưu vào thư mục chung `crawler_output/documents/`, bên trong là thư mục con mang tên chính là **`doc_id`**:

```text
crawler_output/
├── documents/
│   ├── 714/
│   │   ├── content.md         # Toàn bộ nội dung bài viết dạng Markdown sạch (giữ trọn H1, H2, H3, không rác)
│   │   └── metadata.json      # Metadata: ID, URL gốc, Domain, Title, Char count, Timestamp, Status
│   ├── 715/
│   │   ├── content.md
│   │   └── metadata.json
│   └── ...
└── checkpoint.db              # SQLite quản lý index giúp Web Dashboard tìm kiếm O(1) tức thì
```

---

## 3. Khởi Chạy Web Monitoring Dashboard

Khởi chạy máy chủ giao diện web trực quan:
```powershell
python -m vibio_crawler.main --mode dashboard --port 8000
```
Truy cập vào trình duyệt tại: **`http://localhost:8000`**

### Các Tab Chức Năng Trên Giao Diện:
1. **Tổng Quan & Điều Phối (Overview & Control)**:
   - Chọn Nhóm Domain cần cào: `Nhóm 1` (86 domain ổn định), `Nhóm 2` (Long Châu, Wujue), hoặc cào mẫu với giới hạn `limit`.
   - Điều chỉnh thanh trượt Concurrency song song (10 - 100 workers).
   - Nút điều khiển trạng thái: `[ Bắt đầu Crawl ]`, `[ Tạm dừng (Pause) ]`, `[ Tiếp tục (Resume) ]`, `[ Dừng hẳn (Stop) ]`.
   - KPI thời gian thực: Tốc độ (doc/s & URLs/phút), Tổng đã cào, Tỷ lệ thành công, Tổng ký tự text sạch.
   - Biểu đồ thời gian thực (Speed Chart) và thanh tiến trình tổng.
2. **Quản Lý 97 Domain (Domain Groups)**:
   - Danh sách chi tiết 97 domain phân nhóm rõ ràng.
   - Tìm kiếm, lọc theo nhóm, xem % tiến độ, ký tự trung bình.
   - Nút hành động nhanh: **"Crawl riêng domain này"**.
3. **URL Thất Bại & Retry (Failed URLs Hub)**:
   - Bảng liệt kê toàn bộ URL lỗi: Doc ID, Domain, Link URL gốc, Lý do lỗi kỹ thuật chi tiết (`HTTP 403`, `Timeout > 18s`, `Nội dung quá ngắn < 60 ký tự`,...).
   - Cơ chế Retry: Thử cào lại từng URL hoặc bấm **"Cào lại toàn bộ URL lỗi"**.
4. **Thẩm Định & Đối Soát (Side-by-Side Inspector)**:
   - Cột trái: Render nội dung Markdown trực tiếp với cấu trúc H1, H2, H3, in đậm, danh sách.
   - Cột phải: Link URL gốc, thông tin metadata và nút **"Mở URL gốc trên tab mới"** để đối soát trực tiếp.

---

## 4. Chạy Qua Dòng Lệnh CLI (Headless Mode)

Ngoài giao diện Web, hệ thống hỗ trợ chạy hoàn toàn bằng CLI cho máy chủ server / terminal:

* **Chạy cào toàn bộ Nhóm 1**:
  ```powershell
  python -m vibio_crawler.main --mode run --concurrency 50
  ```

* **Chạy mẫu thử nghiệm (Ví dụ 500 URLs)**:
  ```powershell
  python -m vibio_crawler.main --mode sample --limit 500 --concurrency 30
  ```

* **Xem thống kê tiến độ nhanh**:
  ```powershell
  python -m vibio_crawler.main --mode stats
  ```
