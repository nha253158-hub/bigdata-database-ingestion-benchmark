# Big Data Multi-Table Database Ingestion & Benchmark

Dự án nghiên cứu, hiện thực và đánh giá hiệu năng nạp dữ liệu lớn (Big Data Ingestion Benchmark) vào hệ cơ sở dữ liệu quan hệ (RDBMS) sử dụng Python.

---

## 📂 Cấu trúc Repository

```text
├── data/                       # Chứa các file CSV dữ liệu lớn (> 1M và > 2M dòng)
├── schema.sql                  # Định nghĩa cấu trúc DDL nhiều bảng (Users, Orders, Order_Items)
├── generate_mock_data.py       # Script sinh dữ liệu giả lập chuẩn streaming (> 1M - 10M dòng)
├── method1_batch.py            # Cách 1: Ingestion sử dụng Pandas to_sql chunking
├── method2_bulk.py             # Cách 2: Optimized Streaming Ingestion (Low-level + Engine Tuning)
├── benchmark.py                # Script chạy tự động benchmark, kiểm tra tính toàn vẹn và so sánh
├── REPORT.md                   # Báo cáo kỹ thuật chi tiết theo yêu cầu đề bài
├── requirements.txt            # Danh sách thư viện Python phụ thuộc
└── .gitignore                  # Bỏ qua các file database, cache và file dữ liệu dung lượng lớn
```

---

## 🚀 Hướng Dẫn Cài Đặt & Thực Thi

### 1. Cài đặt môi trường
Khuyến nghị sử dụng Python 3.8+ và tạo môi trường ảo (virtual environment):

```bash
# Tạo và kích hoạt môi trường ảo (tùy chọn)
python -m venv venv
# Trên Windows:
venv\Scripts\activate
# Trên Linux/macOS:
source venv/bin/activate

# Cài đặt thư viện phụ thuộc
pip install -r requirements.txt
```

### 2. Sinh dữ liệu giả lập (> 1 triệu dòng / file)
Chạy script sinh dữ liệu chuẩn streaming (sinh 1,000,000 users, 1,000,000 orders và ~2,000,000 order_items):

```bash
python generate_mock_data.py --users 1000000 --orders 1000000
```
> Hoặc có thể tải các bộ dataset thực tế từ Kaggle / NYC TLC / MovieLens theo hướng dẫn trong [REPORT.md](REPORT.md).

### 3. Chạy từng phương pháp nạp dữ liệu
- **Cách 1 (Pandas to_sql chunking):**
  ```bash
  python method1_batch.py
  ```
- **Cách 2 (Optimized Streaming Bulk):**
  ```bash
  python method2_bulk.py
  ```

### 4. Chạy Benchmark tự động & So sánh hiệu năng
Chạy script tổng hợp để tự động đo đạc, so sánh thời gian, RAM tiêu thụ và xác thực tính toàn vẹn dữ liệu:

```bash
python benchmark.py
```

---

## 📊 Tóm tắt kết quả Benchmark
| Tiêu chí | Cách 1: Pandas `to_sql` (Chunksize 50k) | Cách 2: Optimized Bulk (Batch 100k + PRAGMA) | Chênh lệch / Cải thiện |
| :--- | :--- | :--- | :--- |
| **Tốc độ (Throughput)** | ~70,000 - 90,000 dòng/giây | ~400,000 - 550,000 dòng/giây | **Nhanh hơn ~5x - 7x** |
| **Thời gian nạp 4M records** | ~45 - 60 giây | ~7 - 9 giây | **Tiết kiệm ~85% thời gian** |
| **Tiêu thụ RAM tối đa** | Cao (~350 MB - 600 MB) | Cực thấp (~40 MB - 65 MB) | **Giảm > 85% RAM** |
| **Toàn vẹn dữ liệu** | Đầy đủ 100% | Đầy đủ 100% | Chuẩn ACID |

Chi tiết phân tích ưu nhược điểm và lý thuyết ôn tập SQL xem tại [REPORT.md](REPORT.md).
