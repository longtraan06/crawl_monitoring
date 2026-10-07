# ViBioMIR Crawler & Dashboard

Hệ thống crawl bài viết y tế hiệu năng cao kèm Web Dashboard điều phối trực quan.

---

## 1. Cài Đặt Thư Viện (Installation)

### 🪟 Windows
```powershell
pip install fastapi "uvicorn[standard]" curl_cffi trafilatura beautifulsoup4 lxml pyarrow websockets pydantic
```

### 🐧 Linux (Ubuntu / Debian)
```bash
# Cài đặt công cụ nền tảng (nếu chưa có)
sudo apt update && sudo apt install -y python3-pip python3-venv

# Cài đặt packages Python
pip install fastapi "uvicorn[standard]" curl_cffi trafilatura beautifulsoup4 lxml pyarrow websockets pydantic
```

---

## 2. Lệnh Chạy Dashboard

Chạy lệnh sau tại thư mục gốc của dự án:

```bash
python -m vibio_crawler.main --mode dashboard --port 8000
```

Sau khi chạy, mở trình duyệt truy cập:
👉 **`http://localhost:8000`**

---

## 3. Hướng Dẫn Set Path File JSON Custom

Hệ thống hỗ trợ 2 cách thiết lập file JSON tùy chỉnh:

### Cách 1: Thiết lập trên Web Dashboard (Đơn giản nhất)
1. Mở giao diện tại **`http://localhost:8000`**, chọn tab **Crawl theo File JSON**.
2. Nhập đường dẫn tuyệt đối đến file JSON của bạn vào ô input:
   - **Windows:** `C:\Users\username\Desktop\rmu.json`
   - **Linux:** `/home/username/data/rmu.json`
3. Nhấn **"Phân Tích & Thiết Lập"**.
   - Hệ thống sẽ tự động tối ưu Round-Robin và lưu cấu hình (chỉ làm 1 lần duy nhất, các lần sau tự động nhận diện).
   - Sau khi thiết lập xong, nhấn **"Bắt đầu cào File này"**.
   - Nếu muốn đổi file khác, chỉ cần nhấn nút **"Đổi file JSON khác"**.

### Cách 2: Chạy trực tiếp qua dòng lệnh (CLI)
Nếu không dùng giao diện web, bạn có thể chạy thẳng bằng terminal:

```bash
# Windows / Linux:
python -m vibio_crawler.main --mode run --source json --json-path "/duong/dan/den/file.json" --concurrency 50
```

---

### 📝 Định dạng chuẩn của File JSON
File JSON đầu vào cần có cấu trúc danh sách bài viết như sau:

```json
{
  "articles": [
    {
      "id": 1,
      "url": "https://vinmec.com/vi/tin-tuc/bai-viet-mau",
      "url_host": "vinmec.com"
    }
  ]
}
```
*(Hệ thống cũng hỗ trợ định dạng mảng trực tiếp `[{"url": "...", "url_host": "..."}]` hoặc `{"urls": [...]}`)*
