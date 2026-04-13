DROP KEYSPACE IF EXISTS wuliu_t1_cassandra_ks;

CREATE KEYSPACE IF NOT EXISTS wuliu_t1_cassandra_ks
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE wuliu_t1_cassandra_ks;

CREATE TABLE IF NOT EXISTS gps_track_by_vehicle (
  vehicle_id text,
  track_date date,
  track_time timestamp,
  lng double,
  lat double,
  speed_kmh double,
  direction text,
  schedule_id text,
  PRIMARY KEY ((vehicle_id, track_date), track_time)
) WITH CLUSTERING ORDER BY (track_time ASC);

CREATE TABLE IF NOT EXISTS arrival_departure_event (
  schedule_id text,
  event_time timestamp,
  node_id text,
  event_type text,
  remark text,
  PRIMARY KEY ((schedule_id), event_time)
) WITH CLUSTERING ORDER BY (event_time ASC);

CREATE TABLE IF NOT EXISTS route_eta_series (
  schedule_id text,
  record_time timestamp,
  predicted_arrival_time timestamp,
  delay_minutes int,
  PRIMARY KEY ((schedule_id), record_time)
) WITH CLUSTERING ORDER BY (record_time ASC);

CREATE TABLE IF NOT EXISTS sensor_record_by_trip (
  schedule_id text,
  record_time timestamp,
  temperature_value double,
  humidity_value double,
  device_id text,
  sensor_type text,
  alarm_flag text,
  PRIMARY KEY ((schedule_id), record_time)
) WITH CLUSTERING ORDER BY (record_time ASC);

CREATE TABLE IF NOT EXISTS vehicle_heartbeat (
  vehicle_id text,
  heartbeat_time timestamp,
  engine_status text,
  device_status text,
  battery_level int,
  network_status text,
  gps_status text,
  PRIMARY KEY ((vehicle_id), heartbeat_time)
) WITH CLUSTERING ORDER BY (heartbeat_time ASC);

TRUNCATE gps_track_by_vehicle;
TRUNCATE arrival_departure_event;
TRUNCATE route_eta_series;
TRUNCATE sensor_record_by_trip;
TRUNCATE vehicle_heartbeat;

INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0001', '2025-01-01', '2025-01-01 08:05:10', 120.1, 30.8, 72.5, 'OUT', 'SCH_T0001');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0002', '2025-02-04', '2025-02-04 10:12:21', 120.113, 30.811, 73.63, 'OUT', 'SCH_T0002');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0003', '2025-03-07', '2025-03-07 12:19:32', 120.126, 30.822, 74.76, 'OUT', 'SCH_T0003');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0004', '2025-04-10', '2025-04-10 14:26:43', 120.139, 30.833, 75.89, 'OUT', 'SCH_T0004');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0005', '2025-05-13', '2025-05-13 16:33:54', 120.152, 30.844, 77.02, 'OUT', 'SCH_T0005');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0006', '2025-06-16', '2025-06-16 18:40:05', 120.165, 30.855, 78.15, 'OUT', 'SCH_T0006');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0007', '2025-07-19', '2025-07-19 20:47:16', 120.178, 30.866, 79.28, 'OUT', 'SCH_T0007');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0008', '2025-08-22', '2025-08-22 22:54:27', 120.191, 30.877, 80.41, 'OUT', 'SCH_T0008');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0009', '2025-09-25', '2025-09-25 00:01:38', 120.204, 30.888, 81.54, 'OUT', 'SCH_T0009');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0010', '2025-10-28', '2025-10-28 02:08:49', 120.217, 30.899, 82.67, 'OUT', 'SCH_T0010');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0011', '2025-11-03', '2025-11-03 04:15:00', 120.23, 30.91, 83.8, 'OUT', 'SCH_T0011');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0012', '2025-12-06', '2025-12-06 06:22:11', 120.243, 30.921, 84.93, 'OUT', 'SCH_T0012');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0013', '2025-01-09', '2025-01-09 08:29:22', 120.256, 30.932, 86.06, 'OUT', 'SCH_T0013');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0014', '2025-02-12', '2025-02-12 10:36:33', 120.269, 30.943, 87.19, 'OUT', 'SCH_T0014');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0015', '2025-03-15', '2025-03-15 12:43:44', 120.282, 30.954, 88.32, 'OUT', 'SCH_T0015');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0016', '2025-04-18', '2025-04-18 14:50:55', 120.295, 30.965, 89.45, 'OUT', 'SCH_T0016');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0017', '2025-05-21', '2025-05-21 16:57:06', 120.308, 30.976, 90.58, 'OUT', 'SCH_T0017');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0018', '2025-06-24', '2025-06-24 18:04:17', 120.321, 30.987, 91.71, 'OUT', 'SCH_T0018');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0019', '2025-07-27', '2025-07-27 20:11:28', 120.334, 30.998, 92.84, 'OUT', 'SCH_T0019');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0020', '2025-08-02', '2025-08-02 22:18:39', 120.347, 31.009, 93.97, 'OUT', 'SCH_T0020');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0021', '2025-09-05', '2025-09-05 00:25:50', 120.36, 31.02, 95.1, 'OUT', 'SCH_T0021');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0022', '2025-10-08', '2025-10-08 02:32:01', 120.373, 31.031, 96.23, 'OUT', 'SCH_T0022');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0023', '2025-11-11', '2025-11-11 04:39:12', 120.386, 31.042, 97.36, 'OUT', 'SCH_T0023');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0024', '2025-12-14', '2025-12-14 06:46:23', 120.399, 31.053, 98.49, 'OUT', 'SCH_T0024');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0025', '2025-01-17', '2025-01-17 08:53:34', 120.412, 31.064, 99.62, 'OUT', 'SCH_T0025');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0026', '2025-02-20', '2025-02-20 10:00:45', 120.425, 31.075, 100.75, 'OUT', 'SCH_T0026');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0027', '2025-03-23', '2025-03-23 12:07:56', 120.438, 31.086, 101.88, 'OUT', 'SCH_T0027');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0028', '2025-04-26', '2025-04-26 14:14:07', 120.451, 31.097, 103.01, 'OUT', 'SCH_T0028');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0029', '2025-05-01', '2025-05-01 16:21:18', 120.464, 31.108, 104.14, 'OUT', 'SCH_T0029');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0030', '2025-06-04', '2025-06-04 18:28:29', 120.477, 31.119, 105.27, 'OUT', 'SCH_T0030');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0031', '2025-07-07', '2025-07-07 20:35:40', 120.49, 31.13, 106.4, 'OUT', 'SCH_T0031');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0032', '2025-08-10', '2025-08-10 22:42:51', 120.503, 31.141, 107.53, 'OUT', 'SCH_T0032');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0033', '2025-09-13', '2025-09-13 00:49:02', 120.516, 31.152, 108.66, 'OUT', 'SCH_T0033');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0034', '2025-10-16', '2025-10-16 02:56:13', 120.529, 31.163, 109.79, 'OUT', 'SCH_T0034');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0035', '2025-11-19', '2025-11-19 04:03:24', 120.542, 31.174, 110.92, 'OUT', 'SCH_T0035');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0036', '2025-12-22', '2025-12-22 06:10:35', 120.555, 31.185, 112.05, 'OUT', 'SCH_T0036');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0037', '2025-01-25', '2025-01-25 08:17:46', 120.568, 31.196, 113.18, 'OUT', 'SCH_T0037');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0038', '2025-02-28', '2025-02-28 10:24:57', 120.581, 31.207, 114.31, 'OUT', 'SCH_T0038');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0039', '2025-03-03', '2025-03-03 12:31:08', 120.594, 31.218, 115.44, 'OUT', 'SCH_T0039');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0040', '2025-04-06', '2025-04-06 14:38:19', 120.607, 31.229, 116.57, 'OUT', 'SCH_T0040');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0041', '2025-05-09', '2025-05-09 16:45:30', 120.62, 31.24, 117.7, 'OUT', 'SCH_T0041');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0042', '2025-06-12', '2025-06-12 18:52:41', 120.633, 31.251, 118.83, 'OUT', 'SCH_T0042');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0043', '2025-07-15', '2025-07-15 20:59:52', 120.646, 31.262, 119.96, 'OUT', 'SCH_T0043');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0044', '2025-08-18', '2025-08-18 22:06:03', 120.659, 31.273, 121.09, 'OUT', 'SCH_T0044');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0045', '2025-09-21', '2025-09-21 00:13:14', 120.672, 31.284, 122.22, 'OUT', 'SCH_T0045');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0046', '2025-10-24', '2025-10-24 02:20:25', 120.685, 31.295, 123.35, 'OUT', 'SCH_T0046');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0047', '2025-11-27', '2025-11-27 04:27:36', 120.698, 31.306, 124.48, 'OUT', 'SCH_T0047');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0048', '2025-12-02', '2025-12-02 06:34:47', 120.711, 31.317, 125.61, 'OUT', 'SCH_T0048');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0049', '2025-01-05', '2025-01-05 08:41:58', 120.724, 31.328, 126.74, 'OUT', 'SCH_T0049');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0050', '2025-02-08', '2025-02-08 10:48:09', 120.737, 31.339, 127.87, 'OUT', 'SCH_T0050');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0051', '2025-03-11', '2025-03-11 12:55:20', 120.75, 31.35, 129.0, 'OUT', 'SCH_T0051');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0052', '2025-04-14', '2025-04-14 14:02:31', 120.763, 31.361, 130.13, 'OUT', 'SCH_T0052');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0053', '2025-05-17', '2025-05-17 16:09:42', 120.776, 31.372, 131.26, 'OUT', 'SCH_T0053');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0054', '2025-06-20', '2025-06-20 18:16:53', 120.789, 31.383, 132.39, 'OUT', 'SCH_T0054');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0055', '2025-07-23', '2025-07-23 20:23:04', 120.802, 31.394, 133.52, 'OUT', 'SCH_T0055');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0056', '2025-08-26', '2025-08-26 22:30:15', 120.815, 31.405, 134.65, 'OUT', 'SCH_T0056');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0057', '2025-09-01', '2025-09-01 00:37:26', 120.828, 31.416, 135.78, 'OUT', 'SCH_T0057');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0058', '2025-10-04', '2025-10-04 02:44:37', 120.841, 31.427, 136.91, 'OUT', 'SCH_T0058');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0059', '2025-11-07', '2025-11-07 04:51:48', 120.854, 31.438, 138.04, 'OUT', 'SCH_T0059');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0060', '2025-12-10', '2025-12-10 06:58:59', 120.867, 31.449, 139.17, 'OUT', 'SCH_T0060');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0061', '2025-01-13', '2025-01-13 08:05:10', 120.88, 31.46, 140.3, 'OUT', 'SCH_T0061');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0062', '2025-02-16', '2025-02-16 10:12:21', 120.893, 31.471, 141.43, 'OUT', 'SCH_T0062');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0063', '2025-03-19', '2025-03-19 12:19:32', 120.906, 31.482, 142.56, 'OUT', 'SCH_T0063');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0064', '2025-04-22', '2025-04-22 14:26:43', 120.919, 31.493, 143.69, 'OUT', 'SCH_T0064');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0065', '2025-05-25', '2025-05-25 16:33:54', 120.932, 31.504, 144.82, 'OUT', 'SCH_T0065');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0066', '2025-06-28', '2025-06-28 18:40:05', 120.945, 31.515, 145.95, 'OUT', 'SCH_T0066');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0067', '2025-07-03', '2025-07-03 20:47:16', 120.958, 31.526, 147.08, 'OUT', 'SCH_T0067');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0068', '2025-08-06', '2025-08-06 22:54:27', 120.971, 31.537, 148.21, 'OUT', 'SCH_T0068');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0069', '2025-09-09', '2025-09-09 00:01:38', 120.984, 31.548, 149.34, 'OUT', 'SCH_T0069');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0070', '2025-10-12', '2025-10-12 02:08:49', 120.997, 31.559, 150.47, 'OUT', 'SCH_T0070');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0071', '2025-11-15', '2025-11-15 04:15:00', 121.01, 31.57, 151.6, 'OUT', 'SCH_T0071');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0072', '2025-12-18', '2025-12-18 06:22:11', 121.023, 31.581, 152.73, 'OUT', 'SCH_T0072');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0073', '2025-01-21', '2025-01-21 08:29:22', 121.036, 31.592, 153.86, 'OUT', 'SCH_T0073');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0074', '2025-02-24', '2025-02-24 10:36:33', 121.049, 31.603, 154.99, 'OUT', 'SCH_T0074');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0075', '2025-03-27', '2025-03-27 12:43:44', 121.062, 31.614, 156.12, 'OUT', 'SCH_T0075');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0076', '2025-04-02', '2025-04-02 14:50:55', 121.075, 31.625, 157.25, 'OUT', 'SCH_T0076');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0077', '2025-05-05', '2025-05-05 16:57:06', 121.088, 31.636, 158.38, 'OUT', 'SCH_T0077');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0078', '2025-06-08', '2025-06-08 18:04:17', 121.101, 31.647, 159.51, 'OUT', 'SCH_T0078');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0079', '2025-07-11', '2025-07-11 20:11:28', 121.114, 31.658, 160.64, 'OUT', 'SCH_T0079');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0080', '2025-08-14', '2025-08-14 22:18:39', 121.127, 31.669, 161.77, 'OUT', 'SCH_T0080');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0081', '2025-09-17', '2025-09-17 00:25:50', 121.14, 31.68, 162.9, 'OUT', 'SCH_T0081');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0082', '2025-10-20', '2025-10-20 02:32:01', 121.153, 31.691, 164.03, 'OUT', 'SCH_T0082');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0083', '2025-11-23', '2025-11-23 04:39:12', 121.166, 31.702, 165.16, 'OUT', 'SCH_T0083');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0084', '2025-12-26', '2025-12-26 06:46:23', 121.179, 31.713, 166.29, 'OUT', 'SCH_T0084');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0085', '2025-01-01', '2025-01-01 08:53:34', 121.192, 31.724, 167.42, 'OUT', 'SCH_T0085');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0086', '2025-02-04', '2025-02-04 10:00:45', 121.205, 31.735, 168.55, 'OUT', 'SCH_T0086');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0087', '2025-03-07', '2025-03-07 12:07:56', 121.218, 31.746, 169.68, 'OUT', 'SCH_T0087');
INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES
('VEH_T0088', '2025-04-10', '2025-04-10 14:14:07', 121.231, 31.757, 170.81, 'OUT', 'SCH_T0088');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0001', '2025-01-01 08:05:10', 'NODE_0001', 'CREATED', '客户要求到货前电话预约，请提前30分钟联系');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0002', '2025-02-04 10:12:21', 'NODE_0002', 'PICKED', '客户要求到货前电话预约，需拍照留存');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0003', '2025-03-07 12:19:32', 'NODE_0003', 'PACKED', '客户要求到货前电话预约，优先安排月台');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0004', '2025-04-10 14:26:43', 'NODE_0004', 'LOADED', '客户要求到货前电话预约，双人复核后签收');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0005', '2025-05-13 16:33:54', 'NODE_0005', 'DEPARTED', '客户要求到货前电话预约，不得重压');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0006', '2025-06-16 18:40:05', 'NODE_0006', 'ARRIVED_AT_HUB', '客户要求到货前电话预约，保温箱需回收');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0007', '2025-07-19 20:47:16', 'NODE_0007', 'SORTED', '客户要求到货前电话预约，异常立即上报');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0008', '2025-08-22 22:54:27', 'NODE_0008', 'OUT_FOR_DELIVERY', '客户要求到货前电话预约，附签收单');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0009', '2025-09-25 00:01:38', 'NODE_0009', 'SIGNED', '客户要求到货前电话预约，先核对批次再入库');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0010', '2025-10-28 02:08:49', 'NODE_0010', 'RETURNED', '客户要求到货前电话预约，夜间到货请提前报备');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0011', '2025-11-03 04:15:00', 'NODE_0011', 'CREATED', '外箱勿倒置并保持干燥，请提前30分钟联系');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0012', '2025-12-06 06:22:11', 'NODE_0012', 'PICKED', '外箱勿倒置并保持干燥，需拍照留存');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0013', '2025-01-09 08:29:22', 'NODE_0013', 'PACKED', '外箱勿倒置并保持干燥，优先安排月台');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0014', '2025-02-12 10:36:33', 'NODE_0014', 'LOADED', '外箱勿倒置并保持干燥，双人复核后签收');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0015', '2025-03-15 12:43:44', 'NODE_0015', 'DEPARTED', '外箱勿倒置并保持干燥，不得重压');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0016', '2025-04-18 14:50:55', 'NODE_0016', 'ARRIVED_AT_HUB', '外箱勿倒置并保持干燥，保温箱需回收');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0017', '2025-05-21 16:57:06', 'NODE_0017', 'SORTED', '外箱勿倒置并保持干燥，异常立即上报');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0018', '2025-06-24 18:04:17', 'NODE_0018', 'OUT_FOR_DELIVERY', '外箱勿倒置并保持干燥，附签收单');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0019', '2025-07-27 20:11:28', 'NODE_0019', 'SIGNED', '外箱勿倒置并保持干燥，先核对批次再入库');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0020', '2025-08-02 22:18:39', 'NODE_0020', 'RETURNED', '外箱勿倒置并保持干燥，夜间到货请提前报备');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0021', '2025-09-05 00:25:50', 'NODE_0021', 'CREATED', '优先上午派送，请提前30分钟联系');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0022', '2025-10-08 02:32:01', 'NODE_0022', 'PICKED', '优先上午派送，需拍照留存');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0023', '2025-11-11 04:39:12', 'NODE_0023', 'PACKED', '优先上午派送，优先安排月台');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0024', '2025-12-14 06:46:23', 'NODE_0024', 'LOADED', '优先上午派送，双人复核后签收');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0025', '2025-01-17 08:53:34', 'NODE_0025', 'DEPARTED', '优先上午派送，不得重压');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0026', '2025-02-20 10:00:45', 'NODE_0026', 'ARRIVED_AT_HUB', '优先上午派送，保温箱需回收');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0027', '2025-03-23 12:07:56', 'NODE_0027', 'SORTED', '优先上午派送，异常立即上报');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0028', '2025-04-26 14:14:07', 'NODE_0028', 'OUT_FOR_DELIVERY', '优先上午派送，附签收单');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0029', '2025-05-01 16:21:18', 'NODE_0029', 'SIGNED', '优先上午派送，先核对批次再入库');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0030', '2025-06-04 18:28:29', 'NODE_0030', 'RETURNED', '优先上午派送，夜间到货请提前报备');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0031', '2025-07-07 20:35:40', 'NODE_0031', 'CREATED', '到仓后先质检再上架，请提前30分钟联系');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0032', '2025-08-10 22:42:51', 'NODE_0032', 'PICKED', '到仓后先质检再上架，需拍照留存');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0033', '2025-09-13 00:49:02', 'NODE_0033', 'PACKED', '到仓后先质检再上架，优先安排月台');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0034', '2025-10-16 02:56:13', 'NODE_0034', 'LOADED', '到仓后先质检再上架，双人复核后签收');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0035', '2025-11-19 04:03:24', 'NODE_0035', 'DEPARTED', '到仓后先质检再上架，不得重压');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0036', '2025-12-22 06:10:35', 'NODE_0036', 'ARRIVED_AT_HUB', '到仓后先质检再上架，保温箱需回收');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0037', '2025-01-25 08:17:46', 'NODE_0037', 'SORTED', '到仓后先质检再上架，异常立即上报');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0038', '2025-02-28 10:24:57', 'NODE_0038', 'OUT_FOR_DELIVERY', '到仓后先质检再上架，附签收单');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0039', '2025-03-03 12:31:08', 'NODE_0039', 'SIGNED', '到仓后先质检再上架，先核对批次再入库');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0040', '2025-04-06 14:38:19', 'NODE_0040', 'RETURNED', '到仓后先质检再上架，夜间到货请提前报备');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0041', '2025-05-09 16:45:30', 'NODE_0041', 'CREATED', '如遇异常请同步客服群，请提前30分钟联系');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0042', '2025-06-12 18:52:41', 'NODE_0042', 'PICKED', '如遇异常请同步客服群，需拍照留存');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0043', '2025-07-15 20:59:52', 'NODE_0043', 'PACKED', '如遇异常请同步客服群，优先安排月台');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0044', '2025-08-18 22:06:03', 'NODE_0044', 'LOADED', '如遇异常请同步客服群，双人复核后签收');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0045', '2025-09-21 00:13:14', 'NODE_0045', 'DEPARTED', '如遇异常请同步客服群，不得重压');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0046', '2025-10-24 02:20:25', 'NODE_0046', 'ARRIVED_AT_HUB', '如遇异常请同步客服群，保温箱需回收');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0047', '2025-11-27 04:27:36', 'NODE_0047', 'SORTED', '如遇异常请同步客服群，异常立即上报');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0048', '2025-12-02 06:34:47', 'NODE_0048', 'OUT_FOR_DELIVERY', '如遇异常请同步客服群，附签收单');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0049', '2025-01-05 08:41:58', 'NODE_0049', 'SIGNED', '如遇异常请同步客服群，先核对批次再入库');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0050', '2025-02-08 10:48:09', 'NODE_0050', 'RETURNED', '如遇异常请同步客服群，夜间到货请提前报备');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0051', '2025-03-11 12:55:20', 'NODE_0051', 'CREATED', '需核对批次与序列号，请提前30分钟联系');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0052', '2025-04-14 14:02:31', 'NODE_0052', 'PICKED', '需核对批次与序列号，需拍照留存');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0053', '2025-05-17 16:09:42', 'NODE_0053', 'PACKED', '需核对批次与序列号，优先安排月台');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0054', '2025-06-20 18:16:53', 'NODE_0054', 'LOADED', '需核对批次与序列号，双人复核后签收');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0055', '2025-07-23 20:23:04', 'NODE_0055', 'DEPARTED', '需核对批次与序列号，不得重压');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0056', '2025-08-26 22:30:15', 'NODE_0056', 'ARRIVED_AT_HUB', '需核对批次与序列号，保温箱需回收');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0057', '2025-09-01 00:37:26', 'NODE_0057', 'SORTED', '需核对批次与序列号，异常立即上报');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0058', '2025-10-04 02:44:37', 'NODE_0058', 'OUT_FOR_DELIVERY', '需核对批次与序列号，附签收单');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0059', '2025-11-07 04:51:48', 'NODE_0059', 'SIGNED', '需核对批次与序列号，先核对批次再入库');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0060', '2025-12-10 06:58:59', 'NODE_0060', 'RETURNED', '需核对批次与序列号，夜间到货请提前报备');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0061', '2025-01-13 08:05:10', 'NODE_0061', 'CREATED', '收货窗口为工作日9:00-17:00，请提前30分钟联系');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0062', '2025-02-16 10:12:21', 'NODE_0062', 'PICKED', '收货窗口为工作日9:00-17:00，需拍照留存');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0063', '2025-03-19 12:19:32', 'NODE_0063', 'PACKED', '收货窗口为工作日9:00-17:00，优先安排月台');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0064', '2025-04-22 14:26:43', 'NODE_0064', 'LOADED', '收货窗口为工作日9:00-17:00，双人复核后签收');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0065', '2025-05-25 16:33:54', 'NODE_0065', 'DEPARTED', '收货窗口为工作日9:00-17:00，不得重压');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0066', '2025-06-28 18:40:05', 'NODE_0066', 'ARRIVED_AT_HUB', '收货窗口为工作日9:00-17:00，保温箱需回收');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0067', '2025-07-03 20:47:16', 'NODE_0067', 'SORTED', '收货窗口为工作日9:00-17:00，异常立即上报');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0068', '2025-08-06 22:54:27', 'NODE_0068', 'OUT_FOR_DELIVERY', '收货窗口为工作日9:00-17:00，附签收单');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0069', '2025-09-09 00:01:38', 'NODE_0069', 'SIGNED', '收货窗口为工作日9:00-17:00，先核对批次再入库');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0070', '2025-10-12 02:08:49', 'NODE_0070', 'RETURNED', '收货窗口为工作日9:00-17:00，夜间到货请提前报备');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0071', '2025-11-15 04:15:00', 'NODE_0071', 'CREATED', '冷链货物全程2-8℃保存，请提前30分钟联系');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0072', '2025-12-18 06:22:11', 'NODE_0072', 'PICKED', '冷链货物全程2-8℃保存，需拍照留存');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0073', '2025-01-21 08:29:22', 'NODE_0073', 'PACKED', '冷链货物全程2-8℃保存，优先安排月台');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0074', '2025-02-24 10:36:33', 'NODE_0074', 'LOADED', '冷链货物全程2-8℃保存，双人复核后签收');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0075', '2025-03-27 12:43:44', 'NODE_0075', 'DEPARTED', '冷链货物全程2-8℃保存，不得重压');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0076', '2025-04-02 14:50:55', 'NODE_0076', 'ARRIVED_AT_HUB', '冷链货物全程2-8℃保存，保温箱需回收');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0077', '2025-05-05 16:57:06', 'NODE_0077', 'SORTED', '冷链货物全程2-8℃保存，异常立即上报');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0078', '2025-06-08 18:04:17', 'NODE_0078', 'OUT_FOR_DELIVERY', '冷链货物全程2-8℃保存，附签收单');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0079', '2025-07-11 20:11:28', 'NODE_0079', 'SIGNED', '冷链货物全程2-8℃保存，先核对批次再入库');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0080', '2025-08-14 22:18:39', 'NODE_0080', 'RETURNED', '冷链货物全程2-8℃保存，夜间到货请提前报备');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0081', '2025-09-17 00:25:50', 'NODE_0081', 'CREATED', '装卸时轻拿轻放，请提前30分钟联系');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0082', '2025-10-20 02:32:01', 'NODE_0082', 'PICKED', '装卸时轻拿轻放，需拍照留存');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0083', '2025-11-23 04:39:12', 'NODE_0083', 'PACKED', '装卸时轻拿轻放，优先安排月台');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0084', '2025-12-26 06:46:23', 'NODE_0084', 'LOADED', '装卸时轻拿轻放，双人复核后签收');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0085', '2025-01-01 08:53:34', 'NODE_0085', 'DEPARTED', '装卸时轻拿轻放，不得重压');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0086', '2025-02-04 10:00:45', 'NODE_0086', 'ARRIVED_AT_HUB', '装卸时轻拿轻放，保温箱需回收');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0087', '2025-03-07 12:07:56', 'NODE_0087', 'SORTED', '装卸时轻拿轻放，异常立即上报');
INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES
('SCH_T0088', '2025-04-10 14:14:07', 'NODE_0088', 'OUT_FOR_DELIVERY', '装卸时轻拿轻放，附签收单');
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0001', '2025-01-01 08:05:10', '2025-01-01 08:05:10', 5);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0002', '2025-02-04 10:12:21', '2025-02-04 10:12:21', 8);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0003', '2025-03-07 12:19:32', '2025-03-07 12:19:32', 11);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0004', '2025-04-10 14:26:43', '2025-04-10 14:26:43', 14);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0005', '2025-05-13 16:33:54', '2025-05-13 16:33:54', 17);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0006', '2025-06-16 18:40:05', '2025-06-16 18:40:05', 20);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0007', '2025-07-19 20:47:16', '2025-07-19 20:47:16', 23);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0008', '2025-08-22 22:54:27', '2025-08-22 22:54:27', 26);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0009', '2025-09-25 00:01:38', '2025-09-25 00:01:38', 29);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0010', '2025-10-28 02:08:49', '2025-10-28 02:08:49', 32);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0011', '2025-11-03 04:15:00', '2025-11-03 04:15:00', 35);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0012', '2025-12-06 06:22:11', '2025-12-06 06:22:11', 38);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0013', '2025-01-09 08:29:22', '2025-01-09 08:29:22', 41);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0014', '2025-02-12 10:36:33', '2025-02-12 10:36:33', 44);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0015', '2025-03-15 12:43:44', '2025-03-15 12:43:44', 47);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0016', '2025-04-18 14:50:55', '2025-04-18 14:50:55', 50);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0017', '2025-05-21 16:57:06', '2025-05-21 16:57:06', 53);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0018', '2025-06-24 18:04:17', '2025-06-24 18:04:17', 56);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0019', '2025-07-27 20:11:28', '2025-07-27 20:11:28', 59);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0020', '2025-08-02 22:18:39', '2025-08-02 22:18:39', 62);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0021', '2025-09-05 00:25:50', '2025-09-05 00:25:50', 65);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0022', '2025-10-08 02:32:01', '2025-10-08 02:32:01', 68);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0023', '2025-11-11 04:39:12', '2025-11-11 04:39:12', 71);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0024', '2025-12-14 06:46:23', '2025-12-14 06:46:23', 74);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0025', '2025-01-17 08:53:34', '2025-01-17 08:53:34', 77);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0026', '2025-02-20 10:00:45', '2025-02-20 10:00:45', 80);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0027', '2025-03-23 12:07:56', '2025-03-23 12:07:56', 83);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0028', '2025-04-26 14:14:07', '2025-04-26 14:14:07', 86);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0029', '2025-05-01 16:21:18', '2025-05-01 16:21:18', 89);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0030', '2025-06-04 18:28:29', '2025-06-04 18:28:29', 92);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0031', '2025-07-07 20:35:40', '2025-07-07 20:35:40', 95);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0032', '2025-08-10 22:42:51', '2025-08-10 22:42:51', 98);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0033', '2025-09-13 00:49:02', '2025-09-13 00:49:02', 101);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0034', '2025-10-16 02:56:13', '2025-10-16 02:56:13', 104);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0035', '2025-11-19 04:03:24', '2025-11-19 04:03:24', 107);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0036', '2025-12-22 06:10:35', '2025-12-22 06:10:35', 110);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0037', '2025-01-25 08:17:46', '2025-01-25 08:17:46', 113);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0038', '2025-02-28 10:24:57', '2025-02-28 10:24:57', 116);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0039', '2025-03-03 12:31:08', '2025-03-03 12:31:08', 119);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0040', '2025-04-06 14:38:19', '2025-04-06 14:38:19', 122);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0041', '2025-05-09 16:45:30', '2025-05-09 16:45:30', 125);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0042', '2025-06-12 18:52:41', '2025-06-12 18:52:41', 128);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0043', '2025-07-15 20:59:52', '2025-07-15 20:59:52', 131);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0044', '2025-08-18 22:06:03', '2025-08-18 22:06:03', 134);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0045', '2025-09-21 00:13:14', '2025-09-21 00:13:14', 137);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0046', '2025-10-24 02:20:25', '2025-10-24 02:20:25', 140);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0047', '2025-11-27 04:27:36', '2025-11-27 04:27:36', 143);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0048', '2025-12-02 06:34:47', '2025-12-02 06:34:47', 146);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0049', '2025-01-05 08:41:58', '2025-01-05 08:41:58', 149);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0050', '2025-02-08 10:48:09', '2025-02-08 10:48:09', 152);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0051', '2025-03-11 12:55:20', '2025-03-11 12:55:20', 155);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0052', '2025-04-14 14:02:31', '2025-04-14 14:02:31', 158);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0053', '2025-05-17 16:09:42', '2025-05-17 16:09:42', 161);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0054', '2025-06-20 18:16:53', '2025-06-20 18:16:53', 164);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0055', '2025-07-23 20:23:04', '2025-07-23 20:23:04', 167);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0056', '2025-08-26 22:30:15', '2025-08-26 22:30:15', 170);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0057', '2025-09-01 00:37:26', '2025-09-01 00:37:26', 173);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0058', '2025-10-04 02:44:37', '2025-10-04 02:44:37', 176);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0059', '2025-11-07 04:51:48', '2025-11-07 04:51:48', 179);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0060', '2025-12-10 06:58:59', '2025-12-10 06:58:59', 182);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0061', '2025-01-13 08:05:10', '2025-01-13 08:05:10', 185);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0062', '2025-02-16 10:12:21', '2025-02-16 10:12:21', 188);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0063', '2025-03-19 12:19:32', '2025-03-19 12:19:32', 191);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0064', '2025-04-22 14:26:43', '2025-04-22 14:26:43', 194);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0065', '2025-05-25 16:33:54', '2025-05-25 16:33:54', 197);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0066', '2025-06-28 18:40:05', '2025-06-28 18:40:05', 200);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0067', '2025-07-03 20:47:16', '2025-07-03 20:47:16', 203);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0068', '2025-08-06 22:54:27', '2025-08-06 22:54:27', 206);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0069', '2025-09-09 00:01:38', '2025-09-09 00:01:38', 209);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0070', '2025-10-12 02:08:49', '2025-10-12 02:08:49', 212);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0071', '2025-11-15 04:15:00', '2025-11-15 04:15:00', 215);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0072', '2025-12-18 06:22:11', '2025-12-18 06:22:11', 218);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0073', '2025-01-21 08:29:22', '2025-01-21 08:29:22', 221);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0074', '2025-02-24 10:36:33', '2025-02-24 10:36:33', 224);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0075', '2025-03-27 12:43:44', '2025-03-27 12:43:44', 227);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0076', '2025-04-02 14:50:55', '2025-04-02 14:50:55', 230);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0077', '2025-05-05 16:57:06', '2025-05-05 16:57:06', 233);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0078', '2025-06-08 18:04:17', '2025-06-08 18:04:17', 236);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0079', '2025-07-11 20:11:28', '2025-07-11 20:11:28', 239);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0080', '2025-08-14 22:18:39', '2025-08-14 22:18:39', 242);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0081', '2025-09-17 00:25:50', '2025-09-17 00:25:50', 245);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0082', '2025-10-20 02:32:01', '2025-10-20 02:32:01', 248);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0083', '2025-11-23 04:39:12', '2025-11-23 04:39:12', 251);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0084', '2025-12-26 06:46:23', '2025-12-26 06:46:23', 254);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0085', '2025-01-01 08:53:34', '2025-01-01 08:53:34', 257);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0086', '2025-02-04 10:00:45', '2025-02-04 10:00:45', 260);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0087', '2025-03-07 12:07:56', '2025-03-07 12:07:56', 263);
INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES
('SCH_T0088', '2025-04-10 14:14:07', '2025-04-10 14:14:07', 266);
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0001', '2025-01-01 08:05:10', 128.5, 128.5, 'DEV_SCAN_0001', '标准', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0002', '2025-02-04 10:12:21', 129.63, 129.63, 'DEV_SCAN_0002', '加急', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0003', '2025-03-07 12:19:32', 130.76, 130.76, 'DEV_SCAN_0003', '冷链', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0004', '2025-04-10 14:26:43', 131.89, 131.89, 'DEV_SCAN_0004', '电商', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0005', '2025-05-13 16:33:54', 133.02, 133.02, 'DEV_SCAN_0005', '医药', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0006', '2025-06-16 18:40:05', 134.15, 134.15, 'DEV_SCAN_0006', '工业品', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0007', '2025-07-19 20:47:16', 135.28, 135.28, 'DEV_SCAN_0007', '零担', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0008', '2025-08-22 22:54:27', 136.41, 136.41, 'DEV_SCAN_0008', '整车', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0009', '2025-09-25 00:01:38', 137.54, 137.54, 'DEV_SCAN_0009', '标准', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0010', '2025-10-28 02:08:49', 138.67, 138.67, 'DEV_SCAN_0010', '加急', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0011', '2025-11-03 04:15:00', 139.8, 139.8, 'DEV_SCAN_0011', '冷链', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0012', '2025-12-06 06:22:11', 140.93, 140.93, 'DEV_SCAN_0012', '电商', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0013', '2025-01-09 08:29:22', 142.06, 142.06, 'DEV_SCAN_0013', '医药', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0014', '2025-02-12 10:36:33', 143.19, 143.19, 'DEV_SCAN_0014', '工业品', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0015', '2025-03-15 12:43:44', 144.32, 144.32, 'DEV_SCAN_0015', '零担', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0016', '2025-04-18 14:50:55', 145.45, 145.45, 'DEV_SCAN_0016', '整车', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0017', '2025-05-21 16:57:06', 146.58, 146.58, 'DEV_SCAN_0017', '标准', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0018', '2025-06-24 18:04:17', 147.71, 147.71, 'DEV_SCAN_0018', '加急', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0019', '2025-07-27 20:11:28', 148.84, 148.84, 'DEV_SCAN_0019', '冷链', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0020', '2025-08-02 22:18:39', 149.97, 149.97, 'DEV_SCAN_0020', '电商', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0021', '2025-09-05 00:25:50', 151.1, 151.1, 'DEV_SCAN_0021', '医药', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0022', '2025-10-08 02:32:01', 152.23, 152.23, 'DEV_SCAN_0022', '工业品', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0023', '2025-11-11 04:39:12', 153.36, 153.36, 'DEV_SCAN_0023', '零担', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0024', '2025-12-14 06:46:23', 154.49, 154.49, 'DEV_SCAN_0024', '整车', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0025', '2025-01-17 08:53:34', 155.62, 155.62, 'DEV_SCAN_0025', '标准', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0026', '2025-02-20 10:00:45', 156.75, 156.75, 'DEV_SCAN_0026', '加急', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0027', '2025-03-23 12:07:56', 157.88, 157.88, 'DEV_SCAN_0027', '冷链', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0028', '2025-04-26 14:14:07', 159.01, 159.01, 'DEV_SCAN_0028', '电商', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0029', '2025-05-01 16:21:18', 160.14, 160.14, 'DEV_SCAN_0029', '医药', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0030', '2025-06-04 18:28:29', 161.27, 161.27, 'DEV_SCAN_0030', '工业品', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0031', '2025-07-07 20:35:40', 162.4, 162.4, 'DEV_SCAN_0031', '零担', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0032', '2025-08-10 22:42:51', 163.53, 163.53, 'DEV_SCAN_0032', '整车', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0033', '2025-09-13 00:49:02', 164.66, 164.66, 'DEV_SCAN_0033', '标准', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0034', '2025-10-16 02:56:13', 165.79, 165.79, 'DEV_SCAN_0034', '加急', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0035', '2025-11-19 04:03:24', 166.92, 166.92, 'DEV_SCAN_0035', '冷链', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0036', '2025-12-22 06:10:35', 168.05, 168.05, 'DEV_SCAN_0036', '电商', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0037', '2025-01-25 08:17:46', 169.18, 169.18, 'DEV_SCAN_0037', '医药', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0038', '2025-02-28 10:24:57', 170.31, 170.31, 'DEV_SCAN_0038', '工业品', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0039', '2025-03-03 12:31:08', 171.44, 171.44, 'DEV_SCAN_0039', '零担', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0040', '2025-04-06 14:38:19', 172.57, 172.57, 'DEV_SCAN_0040', '整车', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0041', '2025-05-09 16:45:30', 173.7, 173.7, 'DEV_SCAN_0041', '标准', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0042', '2025-06-12 18:52:41', 174.83, 174.83, 'DEV_SCAN_0042', '加急', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0043', '2025-07-15 20:59:52', 175.96, 175.96, 'DEV_SCAN_0043', '冷链', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0044', '2025-08-18 22:06:03', 177.09, 177.09, 'DEV_SCAN_0044', '电商', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0045', '2025-09-21 00:13:14', 178.22, 178.22, 'DEV_SCAN_0045', '医药', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0046', '2025-10-24 02:20:25', 179.35, 179.35, 'DEV_SCAN_0046', '工业品', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0047', '2025-11-27 04:27:36', 180.48, 180.48, 'DEV_SCAN_0047', '零担', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0048', '2025-12-02 06:34:47', 181.61, 181.61, 'DEV_SCAN_0048', '整车', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0049', '2025-01-05 08:41:58', 182.74, 182.74, 'DEV_SCAN_0049', '标准', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0050', '2025-02-08 10:48:09', 183.87, 183.87, 'DEV_SCAN_0050', '加急', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0051', '2025-03-11 12:55:20', 185.0, 185.0, 'DEV_SCAN_0051', '冷链', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0052', '2025-04-14 14:02:31', 186.13, 186.13, 'DEV_SCAN_0052', '电商', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0053', '2025-05-17 16:09:42', 187.26, 187.26, 'DEV_SCAN_0053', '医药', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0054', '2025-06-20 18:16:53', 188.39, 188.39, 'DEV_SCAN_0054', '工业品', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0055', '2025-07-23 20:23:04', 189.52, 189.52, 'DEV_SCAN_0055', '零担', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0056', '2025-08-26 22:30:15', 190.65, 190.65, 'DEV_SCAN_0056', '整车', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0057', '2025-09-01 00:37:26', 191.78, 191.78, 'DEV_SCAN_0057', '标准', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0058', '2025-10-04 02:44:37', 192.91, 192.91, 'DEV_SCAN_0058', '加急', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0059', '2025-11-07 04:51:48', 194.04, 194.04, 'DEV_SCAN_0059', '冷链', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0060', '2025-12-10 06:58:59', 195.17, 195.17, 'DEV_SCAN_0060', '电商', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0061', '2025-01-13 08:05:10', 196.3, 196.3, 'DEV_SCAN_0061', '医药', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0062', '2025-02-16 10:12:21', 197.43, 197.43, 'DEV_SCAN_0062', '工业品', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0063', '2025-03-19 12:19:32', 198.56, 198.56, 'DEV_SCAN_0063', '零担', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0064', '2025-04-22 14:26:43', 199.69, 199.69, 'DEV_SCAN_0064', '整车', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0065', '2025-05-25 16:33:54', 200.82, 200.82, 'DEV_SCAN_0065', '标准', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0066', '2025-06-28 18:40:05', 201.95, 201.95, 'DEV_SCAN_0066', '加急', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0067', '2025-07-03 20:47:16', 203.08, 203.08, 'DEV_SCAN_0067', '冷链', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0068', '2025-08-06 22:54:27', 204.21, 204.21, 'DEV_SCAN_0068', '电商', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0069', '2025-09-09 00:01:38', 205.34, 205.34, 'DEV_SCAN_0069', '医药', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0070', '2025-10-12 02:08:49', 206.47, 206.47, 'DEV_SCAN_0070', '工业品', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0071', '2025-11-15 04:15:00', 207.6, 207.6, 'DEV_SCAN_0071', '零担', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0072', '2025-12-18 06:22:11', 208.73, 208.73, 'DEV_SCAN_0072', '整车', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0073', '2025-01-21 08:29:22', 209.86, 209.86, 'DEV_SCAN_0073', '标准', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0074', '2025-02-24 10:36:33', 210.99, 210.99, 'DEV_SCAN_0074', '加急', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0075', '2025-03-27 12:43:44', 212.12, 212.12, 'DEV_SCAN_0075', '冷链', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0076', '2025-04-02 14:50:55', 213.25, 213.25, 'DEV_SCAN_0076', '电商', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0077', '2025-05-05 16:57:06', 214.38, 214.38, 'DEV_SCAN_0077', '医药', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0078', '2025-06-08 18:04:17', 215.51, 215.51, 'DEV_SCAN_0078', '工业品', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0079', '2025-07-11 20:11:28', 216.64, 216.64, 'DEV_SCAN_0079', '零担', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0080', '2025-08-14 22:18:39', 217.77, 217.77, 'DEV_SCAN_0080', '整车', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0081', '2025-09-17 00:25:50', 218.9, 218.9, 'DEV_SCAN_0081', '标准', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0082', '2025-10-20 02:32:01', 220.03, 220.03, 'DEV_SCAN_0082', '加急', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0083', '2025-11-23 04:39:12', 221.16, 221.16, 'DEV_SCAN_0083', '冷链', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0084', '2025-12-26 06:46:23', 222.29, 222.29, 'DEV_SCAN_0084', '电商', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0085', '2025-01-01 08:53:34', 223.42, 223.42, 'DEV_SCAN_0085', '医药', 'N');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0086', '2025-02-04 10:00:45', 224.55, 224.55, 'DEV_SCAN_0086', '工业品', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0087', '2025-03-07 12:07:56', 225.68, 225.68, 'DEV_SCAN_0087', '零担', 'Y');
INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES
('SCH_T0088', '2025-04-10 14:14:07', 226.81, 226.81, 'DEV_SCAN_0088', '整车', 'N');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0001', '2025-01-01 08:05:10', 'enabled', 'enabled', 95, 'enabled', 'enabled');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0002', '2025-02-04 10:12:21', 'disabled', 'disabled', 88, 'disabled', 'disabled');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0003', '2025-03-07 12:19:32', 'ACTIVE', 'ACTIVE', 80, 'ACTIVE', 'ACTIVE');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0004', '2025-04-10 14:26:43', 'INACTIVE', 'INACTIVE', 72, 'INACTIVE', 'INACTIVE');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0005', '2025-05-13 16:33:54', 'confirmed', 'confirmed', 64, 'confirmed', 'confirmed');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0006', '2025-06-16 18:40:05', 'pending', 'pending', 56, 'pending', 'pending');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0007', '2025-07-19 20:47:16', 'processing', 'processing', 60, 'processing', 'processing');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0008', '2025-08-22 22:54:27', 'synced', 'synced', 60, 'synced', 'synced');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0009', '2025-09-25 00:01:38', 'finished', 'finished', 95, 'finished', 'finished');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0010', '2025-10-28 02:08:49', 'closed', 'closed', 88, 'closed', 'closed');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0011', '2025-11-03 04:15:00', 'open', 'open', 80, 'open', 'open');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0012', '2025-12-06 06:22:11', 'archived', 'archived', 72, 'archived', 'archived');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0013', '2025-01-09 08:29:22', 'frozen', 'frozen', 64, 'frozen', 'frozen');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0014', '2025-02-12 10:36:33', 'draft', 'draft', 56, 'draft', 'draft');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0015', '2025-03-15 12:43:44', 'valid', 'valid', 60, 'valid', 'valid');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0016', '2025-04-18 14:50:55', 'invalid', 'invalid', 60, 'invalid', 'invalid');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0017', '2025-05-21 16:57:06', 'enabled', 'enabled', 95, 'enabled', 'enabled');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0018', '2025-06-24 18:04:17', 'disabled', 'disabled', 88, 'disabled', 'disabled');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0019', '2025-07-27 20:11:28', 'ACTIVE', 'ACTIVE', 80, 'ACTIVE', 'ACTIVE');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0020', '2025-08-02 22:18:39', 'INACTIVE', 'INACTIVE', 72, 'INACTIVE', 'INACTIVE');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0021', '2025-09-05 00:25:50', 'confirmed', 'confirmed', 64, 'confirmed', 'confirmed');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0022', '2025-10-08 02:32:01', 'pending', 'pending', 56, 'pending', 'pending');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0023', '2025-11-11 04:39:12', 'processing', 'processing', 60, 'processing', 'processing');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0024', '2025-12-14 06:46:23', 'synced', 'synced', 60, 'synced', 'synced');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0025', '2025-01-17 08:53:34', 'finished', 'finished', 95, 'finished', 'finished');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0026', '2025-02-20 10:00:45', 'closed', 'closed', 88, 'closed', 'closed');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0027', '2025-03-23 12:07:56', 'open', 'open', 80, 'open', 'open');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0028', '2025-04-26 14:14:07', 'archived', 'archived', 72, 'archived', 'archived');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0029', '2025-05-01 16:21:18', 'frozen', 'frozen', 64, 'frozen', 'frozen');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0030', '2025-06-04 18:28:29', 'draft', 'draft', 56, 'draft', 'draft');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0031', '2025-07-07 20:35:40', 'valid', 'valid', 60, 'valid', 'valid');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0032', '2025-08-10 22:42:51', 'invalid', 'invalid', 60, 'invalid', 'invalid');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0033', '2025-09-13 00:49:02', 'enabled', 'enabled', 95, 'enabled', 'enabled');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0034', '2025-10-16 02:56:13', 'disabled', 'disabled', 88, 'disabled', 'disabled');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0035', '2025-11-19 04:03:24', 'ACTIVE', 'ACTIVE', 80, 'ACTIVE', 'ACTIVE');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0036', '2025-12-22 06:10:35', 'INACTIVE', 'INACTIVE', 72, 'INACTIVE', 'INACTIVE');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0037', '2025-01-25 08:17:46', 'confirmed', 'confirmed', 64, 'confirmed', 'confirmed');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0038', '2025-02-28 10:24:57', 'pending', 'pending', 56, 'pending', 'pending');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0039', '2025-03-03 12:31:08', 'processing', 'processing', 60, 'processing', 'processing');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0040', '2025-04-06 14:38:19', 'synced', 'synced', 60, 'synced', 'synced');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0041', '2025-05-09 16:45:30', 'finished', 'finished', 95, 'finished', 'finished');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0042', '2025-06-12 18:52:41', 'closed', 'closed', 88, 'closed', 'closed');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0043', '2025-07-15 20:59:52', 'open', 'open', 80, 'open', 'open');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0044', '2025-08-18 22:06:03', 'archived', 'archived', 72, 'archived', 'archived');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0045', '2025-09-21 00:13:14', 'frozen', 'frozen', 64, 'frozen', 'frozen');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0046', '2025-10-24 02:20:25', 'draft', 'draft', 56, 'draft', 'draft');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0047', '2025-11-27 04:27:36', 'valid', 'valid', 60, 'valid', 'valid');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0048', '2025-12-02 06:34:47', 'invalid', 'invalid', 60, 'invalid', 'invalid');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0049', '2025-01-05 08:41:58', 'enabled', 'enabled', 95, 'enabled', 'enabled');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0050', '2025-02-08 10:48:09', 'disabled', 'disabled', 88, 'disabled', 'disabled');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0051', '2025-03-11 12:55:20', 'ACTIVE', 'ACTIVE', 80, 'ACTIVE', 'ACTIVE');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0052', '2025-04-14 14:02:31', 'INACTIVE', 'INACTIVE', 72, 'INACTIVE', 'INACTIVE');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0053', '2025-05-17 16:09:42', 'confirmed', 'confirmed', 64, 'confirmed', 'confirmed');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0054', '2025-06-20 18:16:53', 'pending', 'pending', 56, 'pending', 'pending');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0055', '2025-07-23 20:23:04', 'processing', 'processing', 60, 'processing', 'processing');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0056', '2025-08-26 22:30:15', 'synced', 'synced', 60, 'synced', 'synced');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0057', '2025-09-01 00:37:26', 'finished', 'finished', 95, 'finished', 'finished');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0058', '2025-10-04 02:44:37', 'closed', 'closed', 88, 'closed', 'closed');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0059', '2025-11-07 04:51:48', 'open', 'open', 80, 'open', 'open');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0060', '2025-12-10 06:58:59', 'archived', 'archived', 72, 'archived', 'archived');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0061', '2025-01-13 08:05:10', 'frozen', 'frozen', 64, 'frozen', 'frozen');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0062', '2025-02-16 10:12:21', 'draft', 'draft', 56, 'draft', 'draft');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0063', '2025-03-19 12:19:32', 'valid', 'valid', 60, 'valid', 'valid');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0064', '2025-04-22 14:26:43', 'invalid', 'invalid', 60, 'invalid', 'invalid');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0065', '2025-05-25 16:33:54', 'enabled', 'enabled', 95, 'enabled', 'enabled');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0066', '2025-06-28 18:40:05', 'disabled', 'disabled', 88, 'disabled', 'disabled');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0067', '2025-07-03 20:47:16', 'ACTIVE', 'ACTIVE', 80, 'ACTIVE', 'ACTIVE');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0068', '2025-08-06 22:54:27', 'INACTIVE', 'INACTIVE', 72, 'INACTIVE', 'INACTIVE');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0069', '2025-09-09 00:01:38', 'confirmed', 'confirmed', 64, 'confirmed', 'confirmed');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0070', '2025-10-12 02:08:49', 'pending', 'pending', 56, 'pending', 'pending');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0071', '2025-11-15 04:15:00', 'processing', 'processing', 60, 'processing', 'processing');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0072', '2025-12-18 06:22:11', 'synced', 'synced', 60, 'synced', 'synced');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0073', '2025-01-21 08:29:22', 'finished', 'finished', 95, 'finished', 'finished');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0074', '2025-02-24 10:36:33', 'closed', 'closed', 88, 'closed', 'closed');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0075', '2025-03-27 12:43:44', 'open', 'open', 80, 'open', 'open');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0076', '2025-04-02 14:50:55', 'archived', 'archived', 72, 'archived', 'archived');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0077', '2025-05-05 16:57:06', 'frozen', 'frozen', 64, 'frozen', 'frozen');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0078', '2025-06-08 18:04:17', 'draft', 'draft', 56, 'draft', 'draft');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0079', '2025-07-11 20:11:28', 'valid', 'valid', 60, 'valid', 'valid');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0080', '2025-08-14 22:18:39', 'invalid', 'invalid', 60, 'invalid', 'invalid');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0081', '2025-09-17 00:25:50', 'enabled', 'enabled', 95, 'enabled', 'enabled');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0082', '2025-10-20 02:32:01', 'disabled', 'disabled', 88, 'disabled', 'disabled');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0083', '2025-11-23 04:39:12', 'ACTIVE', 'ACTIVE', 80, 'ACTIVE', 'ACTIVE');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0084', '2025-12-26 06:46:23', 'INACTIVE', 'INACTIVE', 72, 'INACTIVE', 'INACTIVE');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0085', '2025-01-01 08:53:34', 'confirmed', 'confirmed', 64, 'confirmed', 'confirmed');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0086', '2025-02-04 10:00:45', 'pending', 'pending', 56, 'pending', 'pending');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0087', '2025-03-07 12:07:56', 'processing', 'processing', 60, 'processing', 'processing');
INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES
('VEH_T0088', '2025-04-10 14:14:07', 'synced', 'synced', 60, 'synced', 'synced');

