# 物流供应链行业数据库 README（多主体、多引擎）

## 1. 项目概览

本目录是《物流供应链行业数据库设计（真实多样样本值版）》的落地工程，覆盖制造商、总包物流、仓储、干线运输、分拨中心、末端配送六类主体。

目标是提供一套可直接初始化的多数据库样本环境，用于：

- 业务建模与演示
- 异构数据库联合查询/同步实验
- 语义映射与跨主体字段统一研究
- 数据治理、质量校验、联邦分析验证

## 2. 业务主体与分工

| 主体 | 角色定位 | 典型职责 |
|---|---|---|
| M1 / M2 | 制造商 | 订单、商品、库存、发货申请 |
| P1 | 第三方物流总包商（3PL） | 运单主单/子单、承运分配、结算、轨迹文档 |
| W1 / W2 | 仓储企业 | 入库、出库、拣货、库存流水、盘点 |
| T1 / T2 | 干线运输企业 | 车辆司机资源、线路计划、班次、运输任务 |
| H1 | 区域分拨中心 | 到件扫描、分拣任务、笼车清单、中转批次、实时缓存 |
| D1 | 末端配送企业 | 派送任务、签收、投诉、回单、移动端离线缓存 |

## 3. 数据库类型与逻辑库清单

| 子系统 | 引擎 | 逻辑库/Keyspace | 主要对象数量 |
|---|---|---|---|
| `m1_mysql` | MySQL | `wuliu_m1_mysql_db` | 7 表 |
| `m2_mysql` | MySQL | `wuliu_m2_mysql_db` | 6 表 |
| `p1_postgresql` | PostgreSQL | `wuliu_p1_postgresql_db`（schema `p1`） | 7 表 |
| `p1_mongodb` | MongoDB | `wuliu_p1_mongodb_db` | 5 集合 |
| `w1_mysql` | MySQL | `wuliu_w1_mysql_db` | 9 表 |
| `w1_clickhouse` | ClickHouse | `wuliu_w1_clickhouse_db` | 5 表 |
| `w2_mysql` | MySQL | `wuliu_w2_mysql_db` | 6 表 |
| `t1_postgresql` | PostgreSQL | `wuliu_t1_postgresql_db` | 10 表 |
| `t1_cassandra` | Cassandra | `wuliu_t1_cassandra_ks` | 5 宽表 |
| `t2_mysql` | MySQL | `wuliu_t2_mysql_db` | 5 表 |
| `h1_postgresql` | PostgreSQL | `wuliu_h1_postgresql_db` | 7 表 |
| `h1_redis` | Redis | `db0` | 4 类 Key 前缀 |
| `d1_mongodb` | MongoDB | `wuliu_d1_mongodb_db` | 6 集合 |
| `d1_sqlite` | SQLite | `d1_sqlite.db` | 5 表 |

## 4. 表结构与集合结构总览

### 4.1 M1（MySQL）

数据库：`wuliu_m1_mysql_db`

| 表名 | 关键字段 | 说明 |
|---|---|---|
| `customer` | `customer_id`, `customer_name`, `customer_type`, `credit_level` | 客户主数据 |
| `product_sku` | `sku_id`, `sku_name`, `product_category_name`, `storage_requirement` | 商品主数据 |
| `warehouse_master` | `warehouse_id`, `warehouse_code`, `temperature_type`, `dispatch_time_window` | 仓库主数据 |
| `sales_order` | `order_id`, `customer_id`, `order_status`, `requested_delivery_time` | 销售订单头 |
| `sales_order_item` | `item_id`, `order_code`, `sku_code`, `order_qty` | 销售订单行 |
| `shipment_request` | `shipment_request_id`, `order_id`, `source_warehouse_id`, `request_status` | 发运申请 |
| `inventory_snapshot` | `snapshot_id`, `warehouse_code`, `product_id`, `available_qty` | 库存快照 |

### 4.2 M2（MySQL）

数据库：`wuliu_m2_mysql_db`

| 表名 | 关键字段 | 说明 |
|---|---|---|
| `client_master` | `client_code`, `client_name`, `client_level`, `status_cd` | 客户主数据 |
| `goods_item` | `product_id`, `product_name`, `cold_flag`, `danger_flag` | 商品主数据 |
| `biz_order` | `biz_order_id`, `sales_no`, `client_code`, `status_cd` | 业务订单头 |
| `biz_order_line` | `line_id`, `biz_order_id`, `product_id`, `qty` | 业务订单行 |
| `send_apply` | `send_apply_no`, `biz_order_id`, `house_id`, `apply_status` | 发货申请 |
| `stock_snapshot` | `snapshot_no`, `house_id`, `product_id`, `free_stock` | 库存快照 |

### 4.3 P1（PostgreSQL）

数据库：`wuliu_p1_postgresql_db`，schema：`p1`

| 表名 | 关键字段 | 说明 |
|---|---|---|
| `shipper_account` | `shipper_id`, `shipper_code`, `shipper_name`, `status` | 货主账号 |
| `client_contract` | `contract_id`, `shipper_id`, `service_scope`, `settlement_cycle` | 客户合同 |
| `master_waybill` | `master_waybill_id`, `master_waybill_no`, `shipper_id`, `waybill_status` | 主运单 |
| `sub_waybill` | `sub_waybill_id`, `master_waybill_id`, `carrier_id`, `sub_waybill_status` | 子运单 |
| `transport_order` | `transport_order_id`, `master_waybill_id`, `transport_type`, `dispatch_status` | 运输委托单 |
| `carrier_assignment` | `assignment_id`, `transport_order_id`, `carrier_id`, `assignment_status` | 承运分配 |
| `freight_settlement` | `settlement_id`, `transport_order_id`, `receivable_amount`, `payable_amount` | 运费结算 |

主要外键链路：

- `client_contract.shipper_id -> shipper_account.shipper_id`
- `master_waybill.shipper_id -> shipper_account.shipper_id`
- `sub_waybill.master_waybill_id -> master_waybill.master_waybill_id`
- `transport_order.master_waybill_id -> master_waybill.master_waybill_id`
- `carrier_assignment.transport_order_id -> transport_order.transport_order_id`
- `freight_settlement.transport_order_id -> transport_order.transport_order_id`

### 4.4 P1（MongoDB）

数据库：`wuliu_p1_mongodb_db`

| 集合 | 主字段/索引 | 说明 |
|---|---|---|
| `event_log` | `event_id` 唯一，`waybill_no` 索引 | 轨迹事件日志 |
| `trace_document` | `trace_id` 唯一，`master_waybill_no` 唯一 | 运单轨迹文档 |
| `exception_report` | `report_id` 唯一，`waybill_no` 索引 | 异常上报 |
| `api_payload_archive` | `archive_id` 唯一，`biz_id`/`call_time` 索引 | 接口报文归档 |
| `receipt_image_meta` | `image_id` 唯一，`waybill_no`/`upload_time` 索引 | 回单影像元数据 |

### 4.5 W1（MySQL）

数据库：`wuliu_w1_mysql_db`

| 表名 | 关键字段 | 说明 |
|---|---|---|
| `warehouse` | `warehouse_id`, `warehouse_code`, `warehouse_type`, `status` | 仓库 |
| `location_bin` | `bin_id`, `warehouse_id`, `bin_code`, `zone_code` | 库位 |
| `inbound_order` | `inbound_order_id`, `warehouse_id`, `inbound_type`, `order_status` | 入库单 |
| `inbound_item` | `item_id`, `inbound_order_id`, `sku_id`, `actual_qty` | 入库明细 |
| `inventory_balance` | `inventory_id`, `warehouse_id`, `bin_id`, `available_qty` | 库存余额 |
| `outbound_order` | `outbound_order_id`, `warehouse_id`, `shipment_request_id`, `order_status` | 出库单 |
| `pick_task` | `pick_task_id`, `outbound_order_id`, `pick_bin_id`, `task_status` | 拣货任务 |
| `inventory_txn` | `txn_id`, `warehouse_id`, `sku_id`, `txn_type`, `qty_delta` | 库存流水 |
| `cycle_count` | `count_id`, `warehouse_id`, `bin_id`, `variance_qty` | 盘点记录 |

### 4.6 W1（ClickHouse）

数据库：`wuliu_w1_clickhouse_db`

| 表名 | 关键字段 | 说明 |
|---|---|---|
| `scan_event_fact` | `event_id`, `warehouse_id`, `event_type`, `event_time` | 扫描事实 |
| `inventory_snapshot_fact` | `snapshot_date`, `warehouse_id`, `sku_id` | 库存日快照 |
| `task_efficiency_fact` | `task_id`, `task_type`, `duration_sec`, `stat_date` | 作业效率 |
| `device_log_fact` | `log_id`, `device_id`, `log_level`, `log_time` | 设备日志 |
| `warehouse_kpi_daily` | `stat_date`, `warehouse_id`, `pick_efficiency`, `on_time_rate` | 仓网 KPI |

### 4.7 W2（MySQL）

数据库：`wuliu_w2_mysql_db`

| 表名 | 关键字段 | 说明 |
|---|---|---|
| `warehouse_ref` | `wh_id`, `wh_code`, `wh_name`, `status_cd` | 仓库主数据 |
| `asn_order` | `asn_no`, `source_no`, `wh_id`, `doc_status` | 预约入库单 |
| `stock_balance` | `stock_id`, `wh_id`, `bin_no`, `qty_on_hand` | 库存余额 |
| `pick_work` | `work_no`, `so_out_no`, `pick_bin_no`, `work_status` | 拣货工单 |
| `so_outbound` | `so_out_no`, `wh_id`, `dispatch_apply_no`, `doc_status` | 出库单 |
| `stock_flow` | `flow_id`, `wh_id`, `flow_type`, `qty_chg`, `flow_tm` | 库存流水 |

### 4.8 T1（PostgreSQL）

数据库：`wuliu_t1_postgresql_db`

| 表名 | 关键字段 | 说明 |
|---|---|---|
| `vehicle` | `vehicle_id`, `plate_no`, `vehicle_type`, `status` | 车辆主数据 |
| `driver` | `driver_id`, `driver_name`, `license_no`, `status` | 司机主数据 |
| `route_plan` | `route_id`, `route_code`, `origin_node_id`, `destination_node_id` | 线路计划 |
| `departure_schedule` | `schedule_id`, `route_id`, `vehicle_id`, `driver_id` | 发车班次 |
| `transport_task` | `task_id`, `schedule_id`, `sub_waybill_id`, `task_status` | 运输任务 |
| `load_manifest` | `manifest_id`, `schedule_id`, `vehicle_id`, `status` | 配载单 |
| `load_manifest_item` | `item_id`, `manifest_id`, `package_no`, `load_qty` | 配载明细 |
| `transport_exception` | `exception_id`, `task_id`, `exception_type`, `process_status` | 运输异常 |
| `fuel_record` | `fuel_record_id`, `vehicle_id`, `schedule_id`, `fuel_amount` | 油耗记录 |
| `maintenance_record` | `maintenance_id`, `vehicle_id`, `maintenance_type`, `cost_amount` | 维保记录 |

主要外键链路：

- `departure_schedule.route_id -> route_plan.route_id`
- `departure_schedule.vehicle_id -> vehicle.vehicle_id`
- `departure_schedule.driver_id -> driver.driver_id`
- `transport_task.schedule_id -> departure_schedule.schedule_id`
- `load_manifest.schedule_id -> departure_schedule.schedule_id`
- `load_manifest_item.manifest_id -> load_manifest.manifest_id`
- `transport_exception.task_id -> transport_task.task_id`
- `fuel_record.vehicle_id -> vehicle.vehicle_id`
- `fuel_record.schedule_id -> departure_schedule.schedule_id`
- `maintenance_record.vehicle_id -> vehicle.vehicle_id`

### 4.9 T1（Cassandra）

Keyspace：`wuliu_t1_cassandra_ks`

| 表名 | 主键模型 | 说明 |
|---|---|---|
| `gps_track_by_vehicle` | `((vehicle_id, track_date), track_time)` | 车辆轨迹点 |
| `arrival_departure_event` | `((schedule_id), event_time)` | 到离站事件 |
| `route_eta_series` | `((schedule_id), record_time)` | ETA 序列 |
| `sensor_record_by_trip` | `((schedule_id), record_time)` | 温湿度/传感器序列 |
| `vehicle_heartbeat` | `((vehicle_id), heartbeat_time)` | 车辆心跳 |

### 4.10 T2（MySQL）

数据库：`wuliu_t2_mysql_db`

| 表名 | 关键字段 | 说明 |
|---|---|---|
| `truck_master` | `truck_id`, `plate_no`, `truck_type`, `status_cd` | 车辆主数据 |
| `driver_master` | `driver_code`, `driver_name`, `license_no`, `status_cd` | 司机主数据 |
| `trip_plan` | `trip_no`, `route_code`, `truck_id`, `driver_id`, `execute_status` | 班次计划 |
| `shipment_task` | `shipment_task_no`, `shipment_no`, `trip_no`, `execute_status` | 运单任务 |
| `truck_load_sheet` | `sheet_no`, `trip_no`, `truck_id`, `status_cd` | 装车单 |

主要外键链路：

- `trip_plan.truck_id -> truck_master.truck_id`
- `trip_plan.driver_id -> driver_master.driver_code`
- `shipment_task.trip_no -> trip_plan.trip_no`
- `shipment_task.truck_id -> truck_master.truck_id`
- `shipment_task.driver_id -> driver_master.driver_code`
- `truck_load_sheet.trip_no -> trip_plan.trip_no`
- `truck_load_sheet.truck_id -> truck_master.truck_id`

### 4.11 H1（PostgreSQL）

数据库：`wuliu_h1_postgresql_db`

| 表名 | 关键字段 | 说明 |
|---|---|---|
| `arrival_scan` | `scan_id`, `package_no`, `station_id`, `scan_time` | 到件扫描 |
| `sorting_chute` | `chute_id`, `station_id`, `chute_code`, `bind_node_id` | 格口配置 |
| `sorting_task` | `sorting_task_id`, `package_no`, `chute_id`, `task_status` | 分拣任务 |
| `transfer_batch` | `batch_id`, `batch_no`, `station_id`, `batch_status` | 中转批次 |
| `cage_manifest` | `cage_id`, `cage_code`, `batch_no`, `loaded_package_qty` | 笼车清单 |
| `detention_package` | `detention_id`, `package_no`, `detention_reason`, `process_status` | 滞留件 |
| `sorting_exception` | `exception_id`, `package_no`, `exception_type`, `process_status` | 分拣异常 |

主要外键链路：

- `sorting_task.chute_id -> sorting_chute.chute_id`
- `cage_manifest.batch_no -> transfer_batch.batch_no`

### 4.12 H1（Redis）

库：`db0`

| Key 前缀 | 结构 | 典型字段 | TTL |
|---|---|---|---|
| `pending_sort_queue:{Hxxxx}` | List | `package_no`, `station_id`, `priority_level`, `enqueue_time` | 7200 秒 |
| `node_throughput_state:{Hxxxx}` | Hash | 节点吞吐状态字段 | 600 秒 |
| `machine_status_cache:{DEV_SCAN_xxxx}` | Hash | 设备状态字段 | 600 秒 |
| `alarm_state_cache:{ID_xxxx}` | Hash | 告警状态字段 | 7200 秒 |

### 4.13 D1（MongoDB）

数据库：`wuliu_d1_mongodb_db`

| 集合 | 主字段/索引 | 说明 |
|---|---|---|
| `delivery_task` | `task_id` 唯一，`task_no` 唯一，`waybill_no` 索引 | 派送任务 |
| `sign_record` | `sign_id` 唯一，`task_id`/`sign_time` 索引 | 签收记录 |
| `complaint_ticket` | `ticket_id` 唯一，`waybill_no`/`process_status` 索引 | 投诉工单 |
| `customer_feedback` | `feedback_id` 唯一，`task_id`/`feedback_time` 索引 | 客户反馈 |
| `proof_of_delivery_meta` | `pod_id` 唯一，`task_id`/`verify_status` 索引 | 回单元数据 |
| `address_parse_doc` | `addr_doc_id` 唯一，地址文本索引 | 地址解析文档 |

### 4.14 D1（SQLite）

数据库文件：`d1_sqlite/d1_sqlite.db`

| 表名 | 关键字段 | 说明 |
|---|---|---|
| `local_task_cache` | `task_id`, `task_no`, `delivery_status`, `sync_status` | 任务离线缓存 |
| `local_sign_cache` | `sign_id`, `task_id`, `sign_result`, `sync_status` | 签收离线缓存 |
| `scan_cache` | `scan_id`, `package_no`, `scan_type`, `sync_status` | 扫描离线缓存 |
| `offline_address_book` | `address_id`, `recipient_phone`, `geo_lng`, `geo_lat` | 离线地址簿 |
| `retry_upload_queue` | `retry_id`, `biz_type`, `payload_json`, `retry_count` | 失败重传队列 |

## 5. 关键数据链路

从业务流程看，核心链路如下：

`制造商(M1/M2订单)` -> `P1主子运单` -> `W1/W2仓配出入库` -> `T1/T2干线运输` -> `H1分拨与缓存` -> `D1末端签收与反馈`

该设计刻意保留跨主体命名差异（如 `sales_order` vs `biz_order`），用于测试语义统一与异构整合。

## 6. 样本数据规则

- 默认初始化样本：每张表/集合/关键缓存维度 88 条（与“真实多样样本值版”文档一致）。
- 可选扩充：`boost_sample_to_100.ps1`、`boost_cassandra_remaining_to_100.ps1` 可扩到 100。
- 样本值覆盖：状态流转、时序字段、节点流转、温控与异常、签收与投诉、离线重传。

## 7. 一键导入与单库导入

### 7.1 批量导入

```powershell
cd "D:\Program Files\BISHE\program\database\wuliu"
.\setup_wuliu_batch1.ps1
.\setup_wuliu_batch2.ps1
.\setup_wuliu_batch3.ps1
.\setup_wuliu_batch4a.ps1
```

### 7.2 单库导入

每个子目录都有 `apply.ps1`，例如：

```powershell
.\m1_mysql\apply.ps1
.\t1_postgresql\apply.ps1
.\d1_mongodb\apply.ps1
```

### 7.3 默认连接参数

- MySQL：`127.0.0.1:3306`，`root/123456`
- PostgreSQL：`127.0.0.1:5432`，`postgres/123456`
- MongoDB：`127.0.0.1:27017`（默认可无账号）
- Redis：`127.0.0.1:6379`，默认脚本密码 `123456`
- Cassandra：`127.0.0.1:9042`
- ClickHouse：`127.0.0.1:9000`，`default`

## 8. 目录说明

- `setup_wuliu_batch1.ps1` 到 `setup_wuliu_batch4a.ps1`：按批次初始化
- `m1_mysql` 到 `d1_sqlite`：各主体数据库脚本目录
- `*_sample_data.*`：样本数据文件
- `SAMPLE_COUNT_REPORT.md`：样本数量报告（用于校验）
- `TODO_BATCH.md`：后续任务

## 9. 备注

- `w1_clickhouse` 当前为 `Log` 引擎实现，便于本地兼容运行。
- T1/T2/T1-Cassandra 的样本已与当前导入脚本对齐，可直接落库。
