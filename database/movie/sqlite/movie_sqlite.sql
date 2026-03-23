PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS local_movie (
  movie_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  alias TEXT,
  genres TEXT,
  imdb_id TEXT,
  douban_score REAL,
  douban_votes INTEGER,
  release_date TEXT,
  year INTEGER,
  tags TEXT,
  updated_time TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS local_person (
  person_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  sex TEXT,
  name_en TEXT,
  name_zh TEXT,
  birth TEXT,
  birthplace TEXT,
  profession TEXT,
  biography TEXT
);

CREATE TABLE IF NOT EXISTS local_user (
  user_md5 TEXT PRIMARY KEY,
  user_nickname TEXT,
  created_time TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS local_movie_person (
  movie_id INTEGER NOT NULL,
  person_id INTEGER NOT NULL,
  relation_type TEXT NOT NULL,
  PRIMARY KEY (movie_id, person_id, relation_type),
  FOREIGN KEY(movie_id) REFERENCES local_movie(movie_id),
  FOREIGN KEY(person_id) REFERENCES local_person(person_id)
);

CREATE TABLE IF NOT EXISTS offline_rating (
  rating_id INTEGER PRIMARY KEY,
  user_md5 TEXT NOT NULL,
  movie_id INTEGER NOT NULL,
  rating INTEGER NOT NULL,
  rating_time TEXT,
  FOREIGN KEY(user_md5) REFERENCES local_user(user_md5),
  FOREIGN KEY(movie_id) REFERENCES local_movie(movie_id)
);

CREATE TABLE IF NOT EXISTS browse_sample (
  browse_id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_md5 TEXT NOT NULL,
  movie_id INTEGER NOT NULL,
  browse_time TEXT NOT NULL,
  source TEXT,
  FOREIGN KEY(user_md5) REFERENCES local_user(user_md5),
  FOREIGN KEY(movie_id) REFERENCES local_movie(movie_id)
);

CREATE TABLE IF NOT EXISTS local_schedule (
  schedule_id INTEGER PRIMARY KEY AUTOINCREMENT,
  movie_id INTEGER NOT NULL,
  cinema_name TEXT,
  hall_name TEXT,
  start_time TEXT NOT NULL,
  ticket_price REAL,
  schedule_status TEXT DEFAULT 'ON_SALE',
  FOREIGN KEY(movie_id) REFERENCES local_movie(movie_id)
);

CREATE TABLE IF NOT EXISTS movie_meta_cache (
  movie_id INTEGER NOT NULL,
  meta_key TEXT NOT NULL,
  meta_value TEXT,
  updated_time TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (movie_id, meta_key),
  FOREIGN KEY(movie_id) REFERENCES local_movie(movie_id)
);

CREATE INDEX IF NOT EXISTS idx_offline_rating_movie_time ON offline_rating(movie_id, rating_time);
CREATE INDEX IF NOT EXISTS idx_browse_sample_user_time ON browse_sample(user_md5, browse_time);
CREATE INDEX IF NOT EXISTS idx_local_schedule_movie_time ON local_schedule(movie_id, start_time);

DELETE FROM movie_meta_cache;
DELETE FROM local_schedule;
DELETE FROM browse_sample;
DELETE FROM offline_rating;
DELETE FROM local_movie_person;
DELETE FROM local_user;
DELETE FROM local_person;
DELETE FROM local_movie;

INSERT INTO local_movie (
  movie_id, name, alias, genres, imdb_id, douban_score, douban_votes, release_date, year, tags
) VALUES
(35267208, '流浪地球2', 'The Wandering Earth II', '科幻/冒险/灾难', 'tt13539646', 8.3, 1325089, '2023-01-22', 2023, '春节档/硬科幻/近五年经典'),
(36779979, '热辣滚烫', 'YOLO', '剧情/喜剧/运动', 'tt30953582', 7.8, 864122, '2024-02-10', 2024, '女性成长/拳击/近五年经典'),
(90000001, '奥本海默', 'Oppenheimer', '剧情/传记/历史', 'tt15398776', 8.8, 1054321, '2023-08-30', 2023, '奥斯卡/传记/近五年经典'),
(90000002, '沙丘2', 'Dune: Part Two', '科幻/动作/冒险', 'tt15239678', 8.5, 702113, '2024-03-08', 2024, 'IMAX/史诗/近五年经典'),
(25845392, '长津湖', 'The Battle at Lake Changjin', '剧情/历史/战争', 'tt13462900', 7.4, 926541, '2021-09-30', 2021, '战争/历史/近五年经典'),
(90000003, '蜘蛛侠：英雄无归', 'Spider-Man: No Way Home', '动作/科幻/奇幻', 'tt10872600', 8.1, 812334, '2021-12-17', 2021, '漫威/多元宇宙/近五年经典'),
(30474725, '满江红', 'Full River Red', '剧情/喜剧/悬疑', 'tt21181940', 7.0, 1214021, '2023-01-22', 2023, '悬疑/古装/近五年经典'),
(34780991, '封神第一部：朝歌风云', 'Creation of the Gods I', '动作/战争/奇幻', 'tt6979756', 7.7, 1025683, '2023-07-20', 2023, '神话史诗/系列电影/近五年经典'),
(90000004, '壮志凌云2：独行侠', 'Top Gun: Maverick', '动作/剧情', 'tt1745960', 8.7, 653402, '2022-05-27', 2022, '飞行实拍/动作/近五年经典'),
(34841067, '你好，李焕英', 'Hi, Mom', '剧情/喜剧/奇幻', 'tt14188440', 7.7, 1456702, '2021-02-12', 2021, '亲情/穿越/近五年经典');

INSERT INTO local_person (person_id, name, sex, name_en, profession, biography) VALUES
(1000525, '吴京', '男', 'Jing Wu', '演员', '中国电影演员。'),
(1276155, '贾玲', '女', 'Ling Jia', '演员/导演', '中国电影导演、演员。'),
(9001101, '基里安·墨菲', '男', 'Cillian Murphy', '演员', '爱尔兰演员。'),
(9001105, '提莫西·查拉梅', '男', 'Timothee Chalamet', '演员', '美国演员。'),
(1325123, '易烊千玺', '男', 'Jackson Yee', '演员', '中国青年演员。'),
(9001109, '汤姆·赫兰德', '男', 'Tom Holland', '演员', '英国演员。'),
(1274259, '沈腾', '男', NULL, '演员', '中国电影演员。'),
(1041022, '费翔', '男', 'Kris Phillips', '演员', '中美双语演员。'),
(9001112, '汤姆·克鲁斯', '男', 'Tom Cruise', '演员', '美国演员。'),
(1316761, '张小斐', '女', NULL, '演员', '中国电影演员。');

INSERT INTO local_user (user_md5, user_nickname, created_time) VALUES
('0ab7e3efacd56983f16503572d2b9915', '恋丶你灬', '2025-01-03 10:22:10'),
('5f4dcc3b5aa765d61d8327deb882cf99', '风继续吹', '2025-01-18 13:09:45'),
('9e107d9d372bb6826bd81d3542a419d6', '月落乌啼', '2025-02-02 19:11:30'),
('e10adc3949ba59abbe56e057f20f883e', '银河补习班', '2025-02-10 08:31:22'),
('25d55ad283aa400af464c76d713c07ad', '海边的曼彻斯特', '2025-02-20 21:46:18'),
('d8578edf8458ce06fbc5bb76a58c5ca4', '花火', '2025-03-01 09:05:42'),
('96e79218965eb72c92a549dd5a330112', '逆光飞行', '2025-03-11 17:27:09'),
('6cb75f652a9b52798eb6cf2201057c73', '河畔青芒', '2025-03-20 15:16:50'),
('c33367701511b4f6020ec61ded352059', '等风来', '2025-03-26 11:03:27'),
('b59c67bf196a4758191e42f76670ceba', '南方有嘉木', '2025-04-03 20:40:11');

INSERT INTO local_movie_person (movie_id, person_id, relation_type) VALUES
(35267208, 1000525, 'ACTOR'),
(36779979, 1276155, 'ACTOR'),
(90000001, 9001101, 'ACTOR'),
(90000002, 9001105, 'ACTOR'),
(25845392, 1325123, 'ACTOR'),
(90000003, 9001109, 'ACTOR'),
(30474725, 1274259, 'ACTOR'),
(34780991, 1041022, 'ACTOR'),
(90000004, 9001112, 'ACTOR'),
(34841067, 1316761, 'ACTOR');

INSERT INTO offline_rating (rating_id, user_md5, movie_id, rating, rating_time) VALUES
(920000001, '0ab7e3efacd56983f16503572d2b9915', 35267208, 5, '2026-03-19 20:18:00'),
(920000002, '5f4dcc3b5aa765d61d8327deb882cf99', 36779979, 4, '2026-03-18 17:10:00'),
(920000003, '9e107d9d372bb6826bd81d3542a419d6', 90000001, 5, '2026-03-17 22:00:00'),
(920000004, 'e10adc3949ba59abbe56e057f20f883e', 90000002, 5, '2026-03-20 10:35:00'),
(920000005, '25d55ad283aa400af464c76d713c07ad', 25845392, 4, '2026-03-19 13:15:00'),
(920000006, 'd8578edf8458ce06fbc5bb76a58c5ca4', 90000003, 5, '2026-03-20 14:28:00'),
(920000007, '96e79218965eb72c92a549dd5a330112', 30474725, 3, '2026-03-20 16:10:00'),
(920000008, '6cb75f652a9b52798eb6cf2201057c73', 34780991, 4, '2026-03-20 17:58:00'),
(920000009, 'c33367701511b4f6020ec61ded352059', 90000004, 4, '2026-03-20 19:53:00'),
(920000010, 'b59c67bf196a4758191e42f76670ceba', 34841067, 4, '2026-03-20 21:06:00');

INSERT INTO browse_sample (user_md5, movie_id, browse_time, source) VALUES
('0ab7e3efacd56983f16503572d2b9915', 35267208, '2026-03-20 09:04:41', 'home_feed'),
('5f4dcc3b5aa765d61d8327deb882cf99', 36779979, '2026-03-20 10:01:14', 'search'),
('9e107d9d372bb6826bd81d3542a419d6', 90000001, '2026-03-20 10:50:35', 'classic_channel'),
('e10adc3949ba59abbe56e057f20f883e', 90000002, '2026-03-20 11:07:42', 'sci_fi_tab'),
('25d55ad283aa400af464c76d713c07ad', 25845392, '2026-03-20 12:01:15', 'recommend'),
('d8578edf8458ce06fbc5bb76a58c5ca4', 90000003, '2026-03-20 12:38:40', 'home_feed'),
('96e79218965eb72c92a549dd5a330112', 30474725, '2026-03-20 13:10:39', 'detail_recall'),
('6cb75f652a9b52798eb6cf2201057c73', 34780991, '2026-03-20 13:35:40', 'home_feed'),
('c33367701511b4f6020ec61ded352059', 90000004, '2026-03-20 14:02:21', 'search_result'),
('b59c67bf196a4758191e42f76670ceba', 34841067, '2026-03-20 14:26:03', 'topic');

INSERT INTO local_schedule (movie_id, cinema_name, hall_name, start_time, ticket_price, schedule_status) VALUES
(35267208, '北京朝阳万达影城', 'IMAX厅', '2026-03-24 10:00:00', 72.00, 'ON_SALE'),
(36779979, '北京朝阳万达影城', '2号厅', '2026-03-24 11:00:00', 56.00, 'ON_SALE'),
(90000001, '上海陆家嘴百丽宫影城', '杜比全景声厅', '2026-03-24 14:00:00', 68.00, 'ON_SALE'),
(90000002, '上海陆家嘴百丽宫影城', '杜比全景声厅', '2026-03-24 18:20:00', 76.00, 'ON_SALE'),
(25845392, '上海陆家嘴百丽宫影城', '4号厅', '2026-03-24 20:10:00', 62.00, 'ON_SALE'),
(90000003, '北京朝阳万达影城', 'IMAX厅', '2026-03-25 10:30:00', 65.00, 'ON_SALE'),
(30474725, '成都太古里CGV影城', '巨幕厅', '2026-03-25 13:40:00', 58.00, 'ON_SALE'),
(34780991, '上海陆家嘴百丽宫影城', '4号厅', '2026-03-25 16:30:00', 64.00, 'ON_SALE'),
(90000004, '北京朝阳万达影城', '2号厅', '2026-03-25 19:30:00', 66.00, 'ON_SALE'),
(34841067, '成都太古里CGV影城', '巨幕厅', '2026-03-25 20:30:00', 52.00, 'ON_SALE');

INSERT INTO movie_meta_cache (movie_id, meta_key, meta_value, updated_time) VALUES
(35267208, 'source', 'douban_dataset', '2026-03-23 19:20:00'),
(35267208, 'cache_version', 'v2026.03.23', '2026-03-23 19:20:00'),
(36779979, 'source', 'douban_dataset', '2026-03-23 19:20:00'),
(36779979, 'cache_version', 'v2026.03.23', '2026-03-23 19:20:00'),
(90000001, 'source', 'douban_dataset', '2026-03-23 19:20:00'),
(90000001, 'cache_version', 'v2026.03.23', '2026-03-23 19:20:00'),
(90000002, 'source', 'douban_dataset', '2026-03-23 19:20:00'),
(90000002, 'cache_version', 'v2026.03.23', '2026-03-23 19:20:00'),
(25845392, 'source', 'douban_dataset', '2026-03-23 19:20:00'),
(25845392, 'cache_version', 'v2026.03.23', '2026-03-23 19:20:00');
