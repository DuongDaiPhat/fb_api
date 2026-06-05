# Hướng dẫn cấu hình Monitoring & Alerting cho Dead Letter Queue (DLQ)

Tài liệu này hướng dẫn cách cấu hình Prometheus và Alertmanager để giám sát topic Kafka `dead_letter` và gửi email cảnh báo cho quản trị viên khi có message thất bại được đẩy vào DLQ.

## 1. Yêu cầu hệ thống
- **Prometheus**: Dùng để thu thập metric (scrape) từ Kafka Exporter.
- **Alertmanager**: Dùng để quản lý cảnh báo (routing, grouping, notification).
- **Kafka Exporter**: Dùng để expose các metric của Kafka (như offset của topic) sang định dạng Prometheus hiểu được.

## 2. Cấu hình Kafka Exporter
Đảm bảo bạn đã cài đặt Kafka Exporter và nó đang lắng nghe trên cổng (ví dụ: `9308`).
Kafka Exporter sẽ cung cấp metric `kafka_consumergroup_lag` và `kafka_topic_partition_current_offset`.

## 3. Cấu hình Prometheus (`prometheus.yml`)
Thêm cấu hình scrape cho Kafka Exporter và đường dẫn tới file rules của Alertmanager:

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

alerting:
  alertmanagers:
    - static_configs:
        - targets: ['alertmanager:9093']

rule_files:
  - 'alert.rules.yml'

scrape_configs:
  - job_name: 'kafka-exporter'
    static_configs:
      - targets: ['kafka-exporter:9308']
```

## 4. Tạo file Alert Rules (`alert.rules.yml`)
Tạo luật cảnh báo để theo dõi sự gia tăng offset của topic `dead_letter`. Nếu offset hiện tại tăng lên, tức là có message mới lọt vào DLQ.

```yaml
groups:
  - name: kafka_dlq_alerts
    rules:
      - alert: DeadLetterQueueNotEmpty
        # Kiểm tra nếu offset của topic dead_letter tăng lên (rate > 0)
        expr: rate(kafka_topic_partition_current_offset{topic="dead_letter"}[1m]) > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Phát hiện message mới trong Dead Letter Queue"
          description: "Topic dead_letter đang nhận thêm message thất bại. Rate: {{ $value }} messages/s. Vui lòng kiểm tra hệ thống!"
```
*Lưu ý:* Biểu thức `rate(kafka_topic_partition_current_offset{topic="dead_letter"}[1m]) > 0` giúp phát hiện bất kỳ sự kiện ghi mới nào vào DLQ trong 1 phút qua.

## 5. Cấu hình Alertmanager (`alertmanager.yml`)
Cấu hình routing để gửi email khi có alert `DeadLetterQueueNotEmpty` được trigger.

```yaml
global:
  smtp_smarthost: 'smtp.gmail.com:587'
  smtp_from: 'alert-system@yourdomain.com'
  smtp_auth_username: 'your-email@gmail.com'
  smtp_auth_password: 'your-app-password'
  smtp_require_tls: true

route:
  receiver: 'admin-email'
  group_wait: 10s
  group_interval: 5m
  repeat_interval: 1h

receivers:
  - name: 'admin-email'
    email_configs:
      - to: 'admin@yourdomain.com'
        send_resolved: false
        html: |
          <h2>Cảnh báo Hệ thống</h2>
          <p><strong>Alert:</strong> {{ .GroupLabels.alertname }}</p>
          <p><strong>Mức độ:</strong> {{ .CommonLabels.severity }}</p>
          <p><strong>Chi tiết:</strong> {{ .CommonAnnotations.description }}</p>
```

## 6. Khởi động và kiểm tra
1. Khởi động lại Prometheus và Alertmanager với cấu hình mới.
2. Publish một message thử nghiệm vào topic `dead_letter` trên Kafka.
3. Chờ 1-2 phút, truy cập Prometheus UI (`http://localhost:9090/alerts`) để xem trạng thái của alert `DeadLetterQueueNotEmpty`.
4. Nếu alert chuyển sang trạng thái **FIRING**, kiểm tra hộp thư email của quản trị viên để xác nhận việc nhận cảnh báo.
