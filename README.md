# Hệ Thống Lưu Trữ Phân Tán Key-Value (Distributed Key-Value Store)

## 📋 Tổng quan

Đây là một hệ thống lưu trữ key-value phân tán được xây dựng bằng Node.js, gRPC và Protocol Buffers. Hệ thống cung cấp khả năng lưu trữ dữ liệu phân tán với tính năng chịu lỗi, nhân bản dữ liệu tự động và cân bằng tải.

### 🏗️ Kiến trúc hệ thống

- **Consistent Hashing**: Phân phối dữ liệu đều trên các node
- **Replication**: Sao chép dữ liệu với replication factor = 2
- **Failure Detection**: Phát hiện lỗi thông qua heartbeat mechanism
- **Load Balancing**: Cân bằng tải với round-robin
- **gRPC Communication**: Giao tiếp hiệu suất cao giữa các thành phần

### 🛠️ Công nghệ sử dụng

- **Node.js**: Runtime environment
- **gRPC**: Framework giao tiếp giữa các service
- **Protocol Buffers**: Định dạng serialization
- **Consistent Hashing**: Thuật toán phân phối dữ liệu

## 📦 Cài đặt

### 1. Yêu cầu hệ thống

- Node.js >= 14.0.0
- npm >= 6.0.0
- Hệ điều hành: Windows/Linux/macOS

### 2. Clone repository

```bash
git clone <repository-url>
cd distributed-chat-system
```

### 3. Cài đặt dependencies

```bash
npm install
```

### 4. Kiểm tra cài đặt

```bash
node --version
npm --version
```

## 🚀 Cách chạy hệ thống

### Phương pháp 1: Chạy cluster đầy đủ

#### Bước 1: Khởi động cluster (3 nodes)

```bash
node server.js
```

Lệnh này sẽ khởi động 3 nodes:
- Node1: localhost:50051
- Node2: localhost:50052  
- Node3: localhost:50053

#### Bước 2: Kết nối client

Mở terminal mới và chạy:

```bash
node client.js
```

### Phương pháp 2: Chạy từng node riêng biệt

#### Bước 1: Khởi động từng node

**Terminal 1 - Node 1:**
```bash
node single-node.js node1 50051
```

**Terminal 2 - Node 2:**
```bash
node single-node.js node2 50052
```

**Terminal 3 - Node 3:**
```bash
node single-node.js node3 50053
```

#### Bước 2: Kết nối client

**Terminal 4 - Client:**
```bash
node client.js
```

## 🖥️ Sử dụng hệ thống

### Giao diện dòng lệnh (CLI)

Sau khi khởi động client, bạn sẽ thấy giao diện:

```
🖥️  KV Store Interactive Client
Commands:
  put <key> <value>  - Store a key-value pair
  get <key>          - Retrieve a value
  delete <key>       - Delete a key
  test <num>         - Run performance test
  nodes              - Show connected nodes
  help               - Show this help
  exit               - Exit the client

kvstore> 
```

### Các lệnh cơ bản

#### 1. Lưu trữ dữ liệu (PUT)

```bash
kvstore> put user:alice "Alice Johnson"
kvstore> put user:bob "Bob Smith"
kvstore> put config:timeout "30"
```

#### 2. Truy xuất dữ liệu (GET)

```bash
kvstore> get user:alice
kvstore> get user:bob
kvstore> get config:timeout
```

#### 3. Xóa dữ liệu (DELETE)

```bash
kvstore> delete user:bob
kvstore> get user:bob  # Sẽ trả về "not found"
```

#### 4. Kiểm tra các nodes

```bash
kvstore> nodes
```

#### 5. Chạy test hiệu suất

```bash
kvstore> test 100  # Test với 100 operations
```

## 🧪 Kiểm thử hệ thống

### Test 1: Chức năng cơ bản

```bash
# Khởi động cluster
node server.js

# Trong terminal khác, chạy demo
node client.js --demo
```

### Test 2: Kiểm tra tính chịu lỗi (Fault Tolerance)

#### Bước 1: Khởi động 3 nodes riêng biệt

```bash
# Terminal 1
node single-node.js node1 50051

# Terminal 2  
node single-node.js node2 50052

# Terminal 3
node single-node.js node3 50053
```

#### Bước 2: Thêm dữ liệu test

```bash
# Terminal 4
node client.js

kvstore> put test:data1 "Important data 1"
kvstore> put test:data2 "Important data 2"
kvstore> put test:data3 "Important data 3"
```

#### Bước 3: Mô phỏng lỗi node

Nhấn `Ctrl+C` ở Terminal 2 để tắt node2

#### Bước 4: Kiểm tra hệ thống vẫn hoạt động

```bash
kvstore> get test:data1  # Vẫn lấy được dữ liệu
kvstore> get test:data2  # Vẫn lấy được dữ liệu
kvstore> put test:data4 "New data after failure"  # Vẫn lưu được
```

#### Bước 5: Thêm node mới

```bash
# Terminal 5 - Node thay thế
node single-node.js node4 50054
```

### Test 3: Kiểm tra hiệu suất

```bash
node client.js --test 1000
```

### Test 4: Kiểm tra phân phối dữ liệu

```bash
kvstore> put item:1 "data1"
kvstore> put item:2 "data2"
kvstore> put item:3 "data3"
# Kiểm tra logs của các nodes để thấy dữ liệu được phân phối
```

## 📊 Giám sát hệ thống

### Xem logs của nodes

Mỗi node sẽ hiển thị thông tin chi tiết:

```
🚀 Starting node1 on port 50051
✅ node1 started successfully!
   Local keys: 0
   Known nodes: 0
   Cluster nodes: node1

PUT request received: user:alice = Alice Johnson
Key user:alice should be stored on nodes: node1, node2
Stored user:alice locally on node node1
Successfully replicated user:alice to node node2
```

### Kiểm tra trạng thái cluster

```bash
kvstore> nodes
```

## 🐛 Xử lý sự cố

### Lỗi thường gặp

#### 1. Lỗi "Failed to connect to any nodes"

**Nguyên nhân:** Các nodes chưa được khởi động

**Giải pháp:**
```bash
# Kiểm tra các nodes đang chạy
netstat -an | grep :50051
netstat -an | grep :50052
netstat -an | grep :50053

# Khởi động lại nodes
node server.js
```

#### 2. Lỗi "All nodes failed"

**Nguyên nhân:** Tất cả nodes đã tắt hoặc bị lỗi

**Giải pháp:**
```bash
# Kiểm tra và khởi động lại cluster
node server.js
```

#### 3. Lỗi "Invalid time value"

**Nguyên nhân:** Vấn đề với timestamp parsing

**Giải pháp:** Đã được fix trong code, timestamp được parse bằng `parseInt()`

#### 4. Port đã được sử dụng

**Nguyên nhân:** Port 50051, 50052, 50053 đã được process khác sử dụng

**Giải pháp:**
```bash
# Windows
netstat -ano | findstr :50051
taskkill /F /PID <PID>

# Linux/macOS
lsof -ti:50051 | xargs kill
```

### Debug logs

Để xem logs chi tiết hơn:

```bash
# Chạy với debug mode
DEBUG=* node server.js
```

## 🔧 Cấu hình nâng cao

### Thay đổi replication factor

Trong file `node.js`, dòng 23:

```javascript
this.consistentHash = new ConsistentHash(3); // Thay đổi từ 2 thành 3
```

### Thay đổi heartbeat frequency

Trong file `node.js`, dòng 31:

```javascript
this.heartbeatFrequency = 3000; // Thay đổi từ 5000ms thành 3000ms
```

### Chạy trên network khác localhost

Trong file `single-node.js`:

```javascript
// Thay đổi địa chỉ seed nodes
let seedNodes = [];
if (nodeId !== 'node1') {
    seedNodes = [{ nodeId: 'node1', address: '192.168.1.100', port: 50051 }];
}
```

## 📈 Mở rộng hệ thống

### Thêm node mới vào cluster đang chạy

```bash
# Thêm node4
node single-node.js node4 50054

# Thêm node5
node single-node.js node5 50055
```

**Lưu ý:** Hệ thống này hiện tại lưu trữ dữ liệu trong memory, dữ liệu sẽ mất khi restart nodes. Để sử dụng production, cần implement data persistence.