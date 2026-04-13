# Logistics Multi-DB Sample Count Report

Generated at: 2026-03-26 (Asia/Shanghai)
Target: 100 rows/documents per business table/collection (or queue length for Redis list keys)

## 1) Manufacturer M1 (MySQL)
- `customer`: 100
- `product_sku`: 100
- `warehouse_master`: 100
- `sales_order`: 100
- `sales_order_item`: 100
- `shipment_request`: 100
- `inventory_snapshot`: 100

## 2) Manufacturer M2 (MySQL)
- `client_master`: 100
- `goods_item`: 100
- `biz_order`: 100
- `biz_order_line`: 100
- `send_apply`: 100
- `stock_snapshot`: 100

## 3) 3PL P1 (PostgreSQL + MongoDB)
- PostgreSQL `p1` schema:
- `shipper_account`: 100
- `client_contract`: 100
- `master_waybill`: 100
- `sub_waybill`: 100
- `transport_order`: 100
- `carrier_assignment`: 100
- `freight_settlement`: 100
- MongoDB `wuliu_p1_mongodb_db`:
- `event_log`: 100
- `trace_document`: 100
- `exception_report`: 100
- `api_payload_archive`: 100
- `receipt_image_meta`: 100

## 4) Warehouse W1 (MySQL + ClickHouse)
- MySQL `wuliu_w1_mysql_db`:
- `warehouse`: 100
- `location_bin`: 100
- `inbound_order`: 100
- `inbound_item`: 100
- `inventory_balance`: 100
- `outbound_order`: 100
- `pick_task`: 100
- `inventory_txn`: 100
- `cycle_count`: 100
- ClickHouse `wuliu_w1_clickhouse_db`:
- `scan_event_fact`: 100
- `inventory_snapshot_fact`: 100
- `task_efficiency_fact`: 100
- `device_log_fact`: 100
- `warehouse_kpi_daily`: 100

## 5) Warehouse W2 (MySQL)
- `warehouse_ref`: 100
- `asn_order`: 100
- `stock_balance`: 100
- `pick_work`: 100
- `so_outbound`: 100
- `stock_flow`: 100

## 6) Trunk T1 (PostgreSQL + Cassandra)
- PostgreSQL `wuliu_t1_postgresql_db`:
- `vehicle`: 100
- `driver`: 100
- `route_plan`: 100
- `departure_schedule`: 100
- `transport_task`: 100
- `load_manifest`: 100
- `load_manifest_item`: 100
- `transport_exception`: 100
- `fuel_record`: 100
- `maintenance_record`: 100
- Cassandra `wuliu_t1_cassandra_ks`:
- `gps_track_by_vehicle`: 100
- `arrival_departure_event`: 100
- `route_eta_series`: 100
- `sensor_record_by_trip`: 100
- `vehicle_heartbeat`: 100

## 7) Trunk T2 (MySQL)
- `truck_master`: 100
- `driver_master`: 100
- `trip_plan`: 100
- `shipment_task`: 100
- `truck_load_sheet`: 100

## 8) Hub H1 (PostgreSQL + Redis)
- PostgreSQL `wuliu_h1_postgresql_db`:
- `arrival_scan`: 100
- `sorting_chute`: 100
- `sorting_task`: 100
- `transfer_batch`: 100
- `cage_manifest`: 100
- `detention_package`: 100
- `sorting_exception`: 100
- Redis `db0` list keys:
- `pending_sort_queue:{H1_ST_SH01}` length: 100
- `pending_sort_queue:{H1_ST_NJ01}` length: 100
- `pending_sort_queue:{H1_ST_TJ01}` length: 100

## 9) Last-mile D1 (MongoDB + SQLite)
- MongoDB `wuliu_d1_mongodb_db`:
- `delivery_task`: 100
- `sign_record`: 100
- `complaint_ticket`: 100
- `customer_feedback`: 100
- `proof_of_delivery_meta`: 100
- `address_parse_doc`: 100
- SQLite `d1_sqlite.db`:
- `local_task_cache`: 100
- `local_sign_cache`: 100
- `scan_cache`: 100
- `offline_address_book`: 100
- `retry_upload_queue`: 100

## Naming-Difference Samples (kept intentionally)
- Same concept, different names:
- `sales_order` (M1) vs `biz_order` (M2)
- `shipment_request` (M1) vs `send_apply` (M2)
- `warehouse_master` (M1) vs `warehouse_ref` (W2)
- Similar token, different meanings:
- `status` in operational tables vs `status_cd` in master data tables
- `batch_no` in hub transfer context vs `lot_no` in warehouse stock context

