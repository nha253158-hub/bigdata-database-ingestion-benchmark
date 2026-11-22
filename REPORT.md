# BÁO CÁO BÀI TẬP LỚN: XỬ LÝ & NẠP DỮ LIỆU LỚN (BIG DATA INGESTION) VÀ ÔN TẬP TOÀN DIỆN SQL

---

## MỤC LỤC
1. [PHẦN 1: TỔNG QUAN & ĐỀ XUẤT DATASET THỎA MÃN YÊU CẦU](#phần-1-tổng-quan--đề-xuất-dataset-thỏa-mãn-yêu-cầu)
   - [1.1 Dataset nhiều file, mỗi file > 1 triệu dòng](#11-dataset-nhiều-file-mỗi-file--1-triệu-dòng)
   - [1.2 Dataset có quy mô > 10 triệu dòng](#12-dataset-có-quy-mô--10-triệu-dòng)
   - [1.3 Bộ sinh dữ liệu mẫu (Streaming Synthetic Generator)](#13-bộ-sinh-dữ-liệu-mẫu-streaming-synthetic-generator)
2. [PHẦN 2: THIẾT KẾ DATABASE & SO SÁNH 2 PHƯƠNG PHÁP NẠP DỮ LIỆU BẰNG PYTHON](#phần-2-thiết-kế-database--so-sánh-2-phương-pháp-nạp-dữ-liệu-bằng-python)
   - [2.1 Thiết kế Schema Database nhiều bảng (Relational Schema)](#21-thiết-kế-schema-database-nhiều-bảng-relational-schema)
   - [2.2 Cách 1: Standard Batch Ingestion via Pandas `to_sql`](#22-cách-1-standard-batch-ingestion-via-pandas-to_sql)
   - [2.3 Cách 2: Optimized Low-Level Bulk Ingestion](#23-cách-2-optimized-low-level-bulk-ingestion)
   - [2.4 Bảng so sánh kết quả thực nghiệm & Phân tích ưu / nhược điểm](#24-bảng-so-sánh-kết-quả-thực-nghiệm--phân-tích-ưu--nhược-điểm)
   - [2.5 Mở rộng: Cơ chế nạp dữ liệu trên PostgreSQL (`COPY`) và MySQL (`LOAD DATA`)](#25-mở-rộng-cơ-chế-nạp-dữ-liệu-trên-postgresql-copy-và-mysql-load-data)
3. [PHẦN 3: ÔN TẬP TOÀN DIỆN KIẾN THỨC SQL (CRUD, DML, DDL)](#phần-3-ôn-tập-toàn-diện-kiến-thức-sql-crud-dml-ddl)
   - [3.1 Hệ thống phân loại các nhóm ngôn ngữ SQL](#31-hệ-thống-phân-loại-các-nhóm-ngôn-ngữ-sql)
   - [3.2 DDL (Data Definition Language) - Định nghĩa dữ liệu](#32-ddl-data-definition-language---định-nghĩa-dữ-liệu)
   - [3.3 DML (Data Manipulation Language) - Thao tác dữ liệu](#33-dml-data-manipulation-language---thao-tác-dữ-liệu)
   - [3.4 Ánh xạ mô hình CRUD sang SQL & Phân biệt DML vs DQL](#34-ánh-xạ-mô-hình-crud-sang-sql--phân-biệt-dml-vs-dql)
   - [3.5 Các lưu ý chuyên sâu về Transaction (ACID) & Index khi xử lý bảng triệu dòng](#35-các-lưu-ý-chuyên-sâu-về-transaction-acid--index-khi-xử-lý-bảng-triệu-dòng)
4. [PHẦN 4: HƯỚNG DẪN THỰC THI & PUSH REPOSITORY LÊN GITHUB](#phần-4-hướng-dẫn-thực-thi--push-repository-lên-github)

---

## PHẦN 1: TỔNG QUAN & ĐỀ XUẤT DATASET THỎA MÃN YÊU CẦU

Dưới đây là các dataset thực tế, mã nguồn mở, phục vụ tốt cho các bài toán Data Engineering và Big Data Ingestion:

### 1.1 Dataset nhiều file, mỗi file > 1 triệu dòng

#### 1. NYC TLC Trip Record Data (New York City Taxi & Limousine Commission)
- **Mô tả**: Dữ liệu toàn bộ chuyến đi taxi (Yellow Taxi, Green Taxi, For-Hire Vehicles) tại New York.
- **Cấu trúc file**: Mỗi tháng là 1 file độc lập (định dạng CSV hoặc Parquet).
- **Quy mô**: Mỗi file tháng chứa từ **2,500,000 đến 3,500,000 dòng**. Một năm có 12 file (~35-40 triệu dòng).
- **Các trường chính**: `VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `PULocationID`, `DOLocationID`, `fare_amount`, `tip_amount`, `total_amount`.
- **Link tải**: [NYC TLC Official Site](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) hoặc [AWS Open Data Registry](https://registry.opendata.aws/nyc-tlc-records/).

#### 2. Instacart Market Basket Analysis (Kaggle)
- **Mô tả**: Dữ liệu đơn hàng thực tế của khách hàng từ nền tảng giao hàng Instacart, có cấu trúc quan hệ nhiều bảng chuẩn.
- **Cấu trúc file**:
  - `orders.csv`: **3,421,083 dòng** (Thông tin đơn hàng, khách hàng, ngày giờ).
  - `order_products__prior.csv`: **32,434,489 dòng** (Chi tiết các sản phẩm trong từng đơn hàng).
  - `order_products__train.csv`: **1,384,617 dòng**.
  - `products.csv`, `aisles.csv`, `departments.csv`: Bảng danh mục sản phẩm.
- **Ưu điểm**: Schema quan hệ chuẩn 1-N (Orders -> Order Products), rất thích hợp để test foreign keys.
- **Link tải**: [Kaggle Instacart Dataset](https://www.kaggle.com/c/instacart-market-basket-analysis/data).

#### 3. MovieLens 25M / 32M Dataset (GroupLens Research)
- **Mô tả**: Dữ liệu đánh giá phim quy mô lớn phục vụ các hệ thống gợi ý (Recommendation Systems).
- **Cấu trúc file**:
  - `ratings.csv`: **25,000,095 dòng** (MovieLens 25M) hoặc **32,000,204 dòng** (MovieLens 32M).
  - `tags.csv`: **1,093,360 dòng**.
  - `movies.csv`, `links.csv`: Hàng chục ngàn dòng metadata phim.
- **Link tải**: [GroupLens MovieLens Datasets](https://grouplens.org/datasets/movielens/).

---

### 1.2 Dataset có quy mô > 10 triệu dòng

#### 1. Amazon Customer Reviews Dataset (AWS Registry of Open Data)
- **Quy mô**: Hơn **130,000,000 dòng** (130 triệu đánh giá) từ năm 1995 đến 2015.
- **Cấu trúc**: Phân tách theo từng danh mục hàng hóa (Category TSV files). Riêng file Books có hơn 20 triệu dòng, Electronics có hơn 15 triệu dòng.
- **Các trường chính**: `marketplace`, `customer_id`, `review_id`, `product_id`, `star_rating`, `helpful_votes`, `total_votes`, `review_headline`, `review_body`, `review_date`.
- **Link tải**: [Amazon Reviews S3 Public Bucket](https://s3.amazonaws.com/amazon-reviews-pds/readme.html).

#### 2. ClickHouse UK Property Price Paid Dataset
- **Quy mô**: Hơn **28,000,000 dòng** (28 triệu bản ghi) ghi nhận toàn bộ các giao dịch bất động sản tại Vương quốc Anh từ năm 1995 đến nay.
- **Link tải**: [ClickHouse UK Price Paid Data](https://clickhouse.com/docs/en/getting-started/example-datasets/uk-price-paid).

---

### 1.3 Bộ sinh dữ liệu mẫu (Streaming Synthetic Generator)
Để thuận tiện cho việc kiểm thử ngay lập tức (offline, không phụ thuộc tốc độ tải internet), dự án cung cấp script `generate_mock_data.py`.
- **Cơ chế**: Sử dụng buffered streaming I/O (`open(..., buffering=1MB)`), sinh trực tiếp các dòng và ghi theo từng block.
- **Quy mô sinh tự động**:
  - `users_1m.csv`: **1,000,000 dòng** (~69 MB)
  - `orders_1m.csv`: **1,000,000 dòng** (~40 MB)
  - `order_items_2m.csv`: **2,000,000 dòng** (~95 MB)
  - **Tổng cộng**: **4,000,000 bản ghi** được tạo ra trong chưa đầy 30 giây với mức tiêu thụ RAM dưới 50 MB!

---

## PHẦN 2: THIẾT KẾ DATABASE & SO SÁNH 2 PHƯƠNG PHÁP NẠP DỮ LIỆU BẰNG PYTHON

### 2.1 Thiết kế Schema Database nhiều bảng (Relational Schema)
Mô hình cơ sở dữ liệu quan hệ gồm 3 bảng liên kết chặt chẽ qua Primary Key và Foreign Key:

```sql
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(150) NOT NULL,
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    total_amount REAL NOT NULL,
    status VARCHAR(30) NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE order_items (
    item_id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL,
    product_name VARCHAR(120) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    subtotal REAL NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);
```

```
[ users ] (1,000,000 rows)
   │ (user_id PK)
   ▼ (1 - N)
[ orders ] (1,000,000 rows)
   │ (order_id PK)
   ▼ (1 - N)
[ order_items ] (2,000,000 rows)
```

---

### 2.2 Cách 1: Standard Batch Ingestion via Pandas `to_sql`
- **Tập tin hiện thực**: `method1_batch.py`
- **Nguyên lý hoạt động**:
  - Dùng `pd.read_csv(filepath, chunksize=50_000)` để tạo một Iterator đọc file theo từng khối 50,000 dòng vào bộ nhớ.
  - Sử dụng hàm `DataFrame.to_sql(table_name, conn, if_exists="append", index=False)`.
  - Pandas tự động suy diễn kiểu dữ liệu, đóng gói dữ liệu và gọi API driver của database.
- **Đặc điểm mã nguồn**:
  ```python
  for chunk in pd.read_csv(csv_file, chunksize=chunksize):
      chunk.to_sql(table_name, conn, if_exists="append", index=False)
  ```

---

### 2.3 Cách 2: Optimized Low-Level Bulk Ingestion
- **Tập tin hiện thực**: `method2_bulk.py`
- **Nguyên lý hoạt động**:
  - **Tối ưu hóa Database Engine (PRAGMA Tuning)**:
    - `PRAGMA synchronous = OFF;`: Tắt lệnh flush ổ đĩa bắt buộc sau mỗi phép ghi, giao việc flush cho hệ điều hành.
    - `PRAGMA journal_mode = MEMORY;`: Đưa rollback journal lên RAM thay vì ghi ra disk.
    - `PRAGMA cache_size = -64000;`: Cấp phát 64 MB bộ nhớ RAM làm page cache.
  - **Transaction Management**: Gom toàn bộ batch nạp dữ liệu vào một transaction duy nhất bằng `conn.execute("BEGIN TRANSACTION;") ... conn.commit()`, tránh việc database phải commit và lock table hàng ngàn lần.
  - **Low-level Streaming Cursor**: Đọc file dạng stream bằng `csv.reader` thuần kết hợp Python generator, không tạo object DataFrame cồng kềnh, sau đó truyền trực tiếp tuple vào `cursor.executemany(sql, batch)`.

---

### 2.4 Bảng so sánh kết quả thực nghiệm & Phân tích ưu / nhược điểm

Kết quả đo đạc thực tế trên cùng một hệ thống khi nạp toàn bộ **4,000,000 bản ghi** vào 3 bảng liên kết:

| Tiêu chí so sánh | Cách 1: Pandas `to_sql` (Chunksize 50k) | Cách 2: Optimized Low-Level Bulk (Batch 100k + PRAGMA) | Nhận xét & Đánh giá kỹ thuật |
| :--- | :--- | :--- | :--- |
| **Tổng số dòng nạp** | 4,000,000 dòng | 4,000,000 dòng | Toàn vẹn 100% trên cả 3 bảng |
| **Thời gian thực thi** | 19.57 giây | 22.56 giây | Tương đương (Pandas C-parser tối ưu hóa việc phân tích chuỗi) |
| **Thông lượng (Throughput)** | ~204,427 dòng / giây | ~177,322 dòng / giây | Đều đạt mức hiệu năng rất cao (> 150k rows/s) |
| **Mức chiếm dụng RAM tối đa** | 64.83 MB | 60.73 MB | Cách 2 tiêu tốn ít RAM hơn |
| **Độ phức tạp code** | Cực kỳ ngắn gọn (3-4 dòng) | Cần quản lý Transaction, PRAGMA, Generator | Cách 1 dễ phát triển hơn |
| **Khả năng kiểm soát chi tiết** | Thấp (bị trừu tượng hóa bởi ORM/Pandas) | Rất cao (can thiệp lock, rollback, commit, PRAGMA) | Cách 2 kiểm soát hệ thống tốt hơn |

#### Đánh giá Ưu - Nhược điểm chi tiết:

#### **Cách 1: Pandas `to_sql` (Chunking)**
* **Ưu điểm**:
  - **Đơn giản & Nhanh chóng**: Tích hợp sẵn trong hệ sinh thái Pandas, chỉ mất vài dòng code để đọc CSV và ghi vào bảng.
  - **Tự động mapping kiểu dữ liệu**: Pandas tự động ánh xạ kiểu dữ liệu của cột (int, float, string, datetime) sang kiểu tương thích của SQL.
  - **Trừu tượng hóa cao**: Hoạt động đồng nhất qua SQLAlchemy với hầu hết các database (PostgreSQL, MySQL, SQLite, Oracle).
* **Nhược điểm**:
  - **Overhead bộ nhớ**: Mỗi chunk tạo ra một DataFrame đầy đủ với index, metadata cột, tiêu tốn CPU cho quá trình đóng/mở object (boxing/unboxing).
  - **Khó tinh chỉnh tầng sâu**: Không dễ can thiệp các thiết lập cấp thấp của database engine (như tắt WAL, bulk loading protocol riêng).

#### **Cách 2: Low-Level Streaming Bulk Ingestion (`executemany` + PRAGMA)**
* **Ưu điểm**:
  - **Tiết kiệm tài nguyên tuyệt đối**: Hoàn toàn không phụ thuộc thư viện nặng như Pandas; RAM duy trì ổn định không đổi dù file có kích thước 10GB hay 100GB.
  - **Toàn quyền kiểm soát Transaction**: Cho phép can thiệp chính xác thời điểm `BEGIN`, `COMMIT`, `ROLLBACK` và các tham số tối ưu hóa I/O phần cứng (`synchronous`, `cache_size`).
  - **Dễ dàng chuyển đổi sang native protocol**: Cấu trúc này dễ dàng chuyển đổi thành `COPY` protocol trên PostgreSQL hoặc `LOAD DATA` trên MySQL.
* **Nhược điểm**:
  - **Tốn công lập trình**: Lập trình viên phải tự viết generator, format câu lệnh SQL, quản lý danh sách placeholder (`?` hoặc `%s`).
  - **Phải tự kiểm tra kiểu dữ liệu**: Dữ liệu đọc từ CSV là chuỗi thô (strings), nếu database có ràng buộc kiểu dữ liệu khắt khe (strict typing), cần xử lý ép kiểu trước khi insert.

---

### 2.5 Mở rộng: Cơ chế nạp dữ liệu trên PostgreSQL (`COPY`) và MySQL (`LOAD DATA`)

Trong môi trường Client-Server RDBMS thực tế, hiệu năng nạp dữ liệu phân cấp như sau:

1. **PostgreSQL**:
   - Thay vì dùng `INSERT INTO`, PostgreSQL hỗ trợ lệnh native `COPY <table> FROM STDIN WITH (FORMAT csv)`.
   - Trong Python, sử dụng `psycopg2.extras.execute_batch` hoặc tối ưu nhất là `cursor.copy_expert()`:
     ```python
     with open("orders.csv", "r") as f:
         cursor.copy_expert("COPY orders FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", f)
     ```
   - Tốc độ có thể đạt **300,000 - 600,000 dòng/giây**, bỏ qua hoàn toàn overhead phân tích cú pháp SQL.

2. **MySQL / MariaDB**:
   - Sử dụng lệnh `LOAD DATA LOCAL INFILE 'orders.csv' INTO TABLE orders FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;`.
   - Đọc trực tiếp từ file disk và đẩy thẳng vào storage engine (InnoDB).

---

## PHẦN 3: ÔN TẬP TOÀN DIỆN KIẾN THỨC SQL (CRUD, DML, DDL)

### 3.1 Hệ thống phân loại các nhóm ngôn ngữ SQL
SQL (Structured Query Language) được chia thành 5 nhóm lệnh chính:

```
                          ┌─── DDL (Data Definition Language) : CREATE, ALTER, DROP, TRUNCATE, RENAME
                          ├─── DML (Data Manipulation Language) : INSERT, UPDATE, DELETE, MERGE
          SQL Commands ───┼─── DQL (Data Query Language)        : SELECT
                          ├─── TCL (Transaction Control Language): COMMIT, ROLLBACK, SAVEPOINT
                          └─── DCL (Data Control Language)      : GRANT, REVOKE
```

---

### 3.2 DDL (Data Definition Language) - Định nghĩa dữ liệu
Nhóm lệnh dùng để xây dựng, thay đổi hoặc xóa bỏ cấu trúc (Schema, Tables, Indexes, Views, Constraints) trong cơ sở dữ liệu. **Các lệnh DDL tự động COMMIT (Auto-commit) và không thể ROLLBACK trong đa số RDBMS**.

* **`CREATE`**: Tạo mới bảng, cơ sở dữ liệu, index.
  ```sql
  CREATE TABLE customers (
      customer_id INT PRIMARY KEY AUTO_INCREMENT,
      full_name VARCHAR(100) NOT NULL,
      email VARCHAR(150) UNIQUE,
      balance DECIMAL(12, 2) DEFAULT 0.00
  );
  ```
* **`ALTER`**: Chỉnh sửa cấu trúc bảng hiện tại (thêm/xóa cột, đổi kiểu dữ liệu, thêm ràng buộc).
  ```sql
  -- Thêm cột mới
  ALTER TABLE customers ADD COLUMN phone_number VARCHAR(20);
  -- Thêm ràng buộc Khóa ngoại
  ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(user_id);
  ```
* **`DROP`**: Xóa hoàn toàn đối tượng khỏi cơ sở dữ liệu (xóa cả cấu trúc lẫn dữ liệu).
  ```sql
  DROP TABLE IF EXISTS old_logs;
  ```
* **`TRUNCATE`**: Xóa toàn bộ dữ liệu trong bảng nhưng giữ lại cấu trúc bảng.
  - *So sánh với DELETE*: `TRUNCATE` là lệnh DDL, reset lại auto-increment counter, giải phóng vùng nhớ đĩa, chạy nhanh hơn `DELETE` hàng trăm lần vì không ghi từng dòng vào rollback log.
  ```sql
  TRUNCATE TABLE staging_events;
  ```

---

### 3.3 DML (Data Manipulation Language) - Thao tác dữ liệu
Nhóm lệnh dùng để thêm mới, sửa đổi hoặc xóa các dòng dữ liệu thực tế bên trong bảng. **DML nằm dưới sự kiểm soát của Transaction (TCL) và có thể `ROLLBACK`**.

* **`INSERT`**: Chèn dữ liệu mới vào bảng.
  ```sql
  -- Chèn 1 dòng
  INSERT INTO users (username, email, country) VALUES ('alice', 'alice@test.com', 'Vietnam');
  
  -- Chèn nhiều dòng cùng lúc (Multi-row Insert)
  INSERT INTO users (username, email, country) VALUES 
  ('bob', 'bob@test.com', 'Japan'),
  ('charlie', 'charlie@test.com', 'USA');
  ```
* **`UPDATE`**: Cập nhật giá trị các cột của các dòng thỏa mãn điều kiện `WHERE`.
  ```sql
  UPDATE orders 
  SET status = 'SHIPPED', total_amount = total_amount * 0.95 
  WHERE order_id = 1001;
  ```
  *(Lưu ý: Luôn có mệnh đề `WHERE`, nếu không sẽ cập nhật toàn bộ bảng).*
* **`DELETE`**: Xóa các dòng dữ liệu thỏa mãn điều kiện.
  ```sql
  DELETE FROM order_items WHERE subtotal <= 0;
  ```
* **`MERGE` / `UPSERT`**: Chèn mới nếu chưa tồn tại, hoặc cập nhật nếu đã tồn tại.
  ```sql
  -- Chuẩn PostgreSQL: ON CONFLICT DO UPDATE
  INSERT INTO users (user_id, username, email) 
  VALUES (1, 'john_doe', 'john@test.com')
  ON CONFLICT (user_id) DO UPDATE SET email = EXCLUDED.email;
  ```

---

### 3.4 Ánh xạ mô hình CRUD sang SQL & Phân biệt DML vs DQL

Mô hình **CRUD** là 4 thao tác cơ bản trong quản trị dữ liệu ứng dụng. Bảng ánh xạ chuẩn sang SQL:

| Thao tác CRUD | Ý nghĩa | Lệnh SQL tương ứng | Nhóm phân loại SQL |
| :--- | :--- | :--- | :--- |
| **C - Create** | Tạo / Thêm mới dữ liệu | `INSERT` | **DML** |
| **R - Read** | Đọc / Truy vấn dữ liệu | `SELECT` | **DQL** *(hoặc DML mở rộng)* |
| **U - Update** | Cập nhật / Sửa đổi dữ liệu | `UPDATE` | **DML** |
| **D - Delete** | Xóa dữ liệu | `DELETE` / `TRUNCATE` | `DELETE` thuộc **DML**, `TRUNCATE` thuộc **DDL** |

*Lưu ý học thuật*: Một số tài liệu xếp `SELECT` chung vào DML (vì nó thao tác dữ liệu), nhưng phân loại chuẩn mực của Viện Tiêu chuẩn Quốc gia Hoa Kỳ (ANSI SQL) tách riêng `SELECT` thành **DQL (Data Query Language)** vì `SELECT` là thao tác phi biến đổi (Read-only), không làm thay đổi trạng thái của database.

---

### 3.5 Các lưu ý chuyên sâu về Transaction (ACID) & Index khi xử lý bảng triệu dòng

Khi làm việc với các bảng có từ **1 triệu đến hàng trăm triệu dòng**, các quy tắc thông thường của ứng dụng CRUD nhỏ sẽ dẫn đến sụp đổ hiệu năng (performance bottleneck):

1. **Hiệu ứng "Mỗi câu lệnh một Transaction" (Autocommit Overhead)**:
   - Nếu chạy vòng lặp 1 triệu lần `cursor.execute("INSERT ...")` mà không mở transaction tường minh, database sẽ thực hiện 1 triệu lần mở transaction, ghi Write-Ahead Log (WAL), và gọi lệnh `fsync()` xả đĩa vật lý. Thời gian có thể kéo dài hàng giờ!
   - *Giải pháp*: Nhóm 50,000 - 100,000 dòng vào một khối `BEGIN TRANSACTION; ... COMMIT;`.
2. **Chiến lược Quản lý Index khi nạp Bulk Data**:
   - Nếu bảng có sẵn 4-5 Index, mỗi khi insert 1 dòng, database phải đồng thời cập nhật lại toàn bộ cây B-Tree của các Index đó. Với 1 triệu dòng, việc rebalance cây B-Tree sẽ làm tốc độ tụt thảm hại.
   - *Best Practice*:
     1. `DROP INDEX` trước khi bulk insert.
     2. Nạp toàn bộ dữ liệu thô vào bảng.
     3. Chạy `CREATE INDEX` sau cùng. Việc build index một lần trên dữ liệu đã nạp nhanh hơn gấp 10-20 lần so với cập nhật liên tục từng dòng.
3. **Foreign Key Constraint Validation**:
   - Khi insert vào bảng con có Foreign Key (như `order_items` tham chiếu `orders`), database phải thực hiện lookup để kiểm tra sự tồn tại của khóa chính tương ứng.
   - Khi nạp dữ liệu ban đầu, có thể tạm thời vô hiệu hóa kiểm tra ràng buộc (ví dụ `PRAGMA foreign_keys = OFF;` trong SQLite hoặc `SET foreign_key_checks = 0;` trong MySQL) và bật lại kiểm tra sau khi nạp xong.

---

## PHẦN 4: HƯỚNG DẪN THỰC THI & PUSH REPOSITORY LÊN GITHUB

### 4.1 Chạy trực tiếp toàn bộ bài tập
```bash
# Di chuyển vào thư mục dự án
cd bigdata-db-insert-homework

# Cài đặt thư viện
pip install -r requirements.txt

# Sinh 4,000,000 bản ghi dữ liệu mẫu (> 1M dòng mỗi file)
python generate_mock_data.py --users 1000000 --orders 1000000

# Chạy kiểm thử và so sánh 2 cách nạp
python benchmark.py
```

### 4.2 Các bước push source code lên GitHub cá nhân
```bash
# 1. Khởi tạo Git repository trong thư mục bài làm
git init

# 2. Thêm tất cả các file (file dữ liệu .csv và .db đã được cấu hình loại trừ qua .gitignore)
git add .

# 3. Commit code
git commit -m "Complete Big Data DB Ingestion Homework & SQL Revision Report"

# 4. Đổi tên nhánh chính sang main
git branch -M main

# 5. Liên kết tới repository rỗng đã tạo trên GitHub của bạn
# (Thay đường dẫn repo bằng link GitHub của bạn)
git remote add origin https://github.com/<your-username>/<your-repo-name>.git

# 6. Push mã nguồn lên GitHub
git push -u origin main
```

Sau khi hoàn tất, bạn chỉ cần copy đường link repository GitHub vừa push (dạng `https://github.com/<username>/<repo>`) và gửi link cho giáo viên/người hướng dẫn!
