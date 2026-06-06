# Hệ thống quản lý Facebook Page phân tán & Tự động hóa AI

Hệ thống phân tán tích hợp Facebook Graph API, xử lý sự kiện theo thời gian thực qua Kafka và ứng dụng AI (Google Gemini) để phân tích cảm xúc người dùng, từ đó đưa ra quyết định tự động phản hồi (Auto-reply), ẩn bình luận (Hide), hoặc đưa vào hàng chờ duyệt (Pending Review).

## Kiến trúc hệ thống
Hệ thống được thiết kế theo mô hình Microservices với giao tiếp Event-Driven qua Kafka:
- **Webhook Service (Node.js - Port 3001):** Điểm tiếp nhận sự kiện từ Facebook. Xác minh chữ ký bảo mật HMAC-SHA256 (để chống giả mạo) và đẩy sự kiện thô vào Kafka.
- **Core Service (Python - Port 3002):** Chịu trách nhiệm phân tích dữ liệu. Lấy sự kiện từ Kafka, gọi Gemini AI để trích xuất Intent (Ý định) & Sentiment (Cảm xúc). Áp dụng luật tự động hóa và đẩy lệnh hành động (Action) vào Kafka.
- **Backend API (Java/Spring Boot - Port 3000):** Tương tác trực tiếp với Facebook Graph API. Xử lý khóa Idempotency với DB để không bao giờ gửi phản hồi trùng lặp dù Kafka có gửi lại message. Đồng thời cung cấp REST API cho Frontend.
- **Retry Service (Java - Port 3003):** Đảm bảo tính chống chịu lỗi của hệ thống. Thực hiện retry tự động theo thuật toán Exponential Backoff (chờ tăng dần) khi việc gọi Facebook API thất bại. Quản lý luồng Dead Letter Queue (DLQ).
- **Frontend (Port 5173):** Giao diện quản trị, xem danh sách bình luận, thống kê và lịch sử tương tác.
- **Hạ tầng cơ sở:** Kafka, Zookeeper, PostgreSQL, Prometheus, Alertmanager.

## Yêu cầu môi trường
- **Docker** và **Docker Compose** (bắt buộc)
- Một ứng dụng và Page đã tạo trên [Facebook Developers](https://developers.facebook.com/) (để lấy Token, App Secret)
- Khóa [Google AI Studio](https://aistudio.google.com/) API Key (để dùng Gemini)

## Hướng dẫn cài đặt và chạy hệ thống

### Bước 1: Cấu hình biến môi trường
Tạo hoặc chỉnh sửa file `.env` tại thư mục gốc của dự án với các thông số của bạn:
```env
# Facebook App & Page Configuration
PAGE_ACCESS_TOKEN=your_facebook_page_access_token_here
APP_SECRET=your_facebook_app_secret_here
VERIFY_TOKEN=your_custom_verify_token_for_webhook

# Google Gemini AI
GEMINI_API_KEY=your_gemini_api_key_here

# Cloudinary (Quản lý hình ảnh - Tùy chọn)
CLOUDINARY_CLOUD_NAME=your_cloud_name
CLOUDINARY_API_KEY=your_api_key
CLOUDINARY_API_SECRET=your_api_secret
```

### Bước 2: Khởi động hệ thống
Mở terminal/cmd tại thư mục chứa file `docker-compose.yml` và chạy lệnh sau để build và khởi động tất cả container:
```bash
docker compose up -d --build
```

### Bước 3: Đăng ký Webhook với Facebook
- Truy cập Facebook Developer Dashboard của ứng dụng.
- Tìm mục **Webhooks**, thiết lập Callback URL trỏ tới đường dẫn Webhook Service của bạn (Lưu ý FB yêu cầu `https`, bạn có thể dùng `ngrok` để map port 3001 ở local ra internet).
- Điền mã xác nhận trùng với biến `VERIFY_TOKEN` trong file `.env`.
- **Subscribe** (Đăng ký) nhận 2 trường: `messages` (nếu làm Chatbot) và `feed` (để nhận Comment của Page).

## Truy cập các thành phần nội bộ

Hệ thống cung cấp một số công cụ quản trị chạy sẵn ở local:

| Dịch vụ | URL Local | Chức năng |
|---|---|---|
| **Frontend Dashboard** | `http://localhost:5173` | Giao diện cho Admin quản lý |
| **Backend API** | `http://localhost:3000` | Gateway & REST API trung tâm |
| **Kafka UI** | `http://localhost:8080` | Quản lý/Theo dõi topic, message, consumer group của Kafka |
| **Prometheus** | `http://localhost:9090` | Xem metrics hệ thống và các cảnh báo đang kích hoạt |
| **Alertmanager** | `http://localhost:9093` | Nơi quản lý việc gửi Email báo động hệ thống |

## Cơ chế hoạt động luồng thời gian thực
1. Người dùng bình luận trên bài viết ở Facebook Page.
2. Facebook gọi API đến `Webhook Service` -> Service xác minh chữ ký, chuẩn hóa JSON -> Đẩy vào Kafka topic `raw_events`.
3. `Core Service` consume `raw_events` -> Gửi nội dung cho AI phân tích -> Phân loại (vd: Xin lỗi nếu Tiêu cực, Cảm ơn nếu Tích cực, Ẩn nếu Spam) -> Đẩy lệnh vào topic `reply_commands`.
4. `Backend API` consume `reply_commands` -> Kiểm tra khóa `idempotency` trong PostgreSQL -> Lên mạng gọi gọi Facebook Graph API phản hồi khách hàng.
5. **Xử lý lỗi:** Nếu gọi Facebook API thất bại, `Backend API` đẩy message sang topic `send_failed`. `Retry Service` sẽ đọc và lùi thời gian thử lại (1s, 2s, 4s). Quá 3 lần mà vẫn lỗi, message bị chuyển vĩnh viễn vào topic `dead_letter`.
6. **Cảnh báo vận hành:** Prometheus phát hiện message rơi vào `dead_letter`, ngay lập tức Alertmanager kích hoạt thông báo gửi về Email của bộ phận IT.

## Các lệnh Docker hữu ích
- Khởi động lại và nạp cấu hình `.env` mới (cực kỳ quan trọng khi thay đổi API Key):
  ```bash
  docker compose up -d --force-recreate
  ```
- Xem log của một service cụ thể (vd: xem AI xử lý):
  ```bash
  docker compose logs -f core-service
  ```
- Xem danh sách các dịch vụ đang chạy:
  ```bash
  docker compose ps
  ```
- Tắt hệ thống:
  ```bash
  docker compose down
  ```
- Tắt hệ thống và **xóa trắng** cơ sở dữ liệu (PostgreSQL, Kafka Logs):
  ```bash
  docker compose down -v
  ```
