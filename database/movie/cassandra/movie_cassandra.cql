DROP KEYSPACE IF EXISTS movie_cassandra_ks;

CREATE KEYSPACE IF NOT EXISTS movie_cassandra_ks
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE movie_cassandra_ks;

CREATE TABLE IF NOT EXISTS movie_browse_event (
  user_md5 text,
  event_date date,
  event_time timeuuid,
  movie_id bigint,
  region_code text,
  device_type text,
  PRIMARY KEY ((user_md5, event_date), event_time, movie_id)
) WITH CLUSTERING ORDER BY (event_time DESC, movie_id ASC);

CREATE TABLE IF NOT EXISTS movie_click_event (
  user_md5 text,
  event_date date,
  event_time timeuuid,
  movie_id bigint,
  source text,
  PRIMARY KEY ((user_md5, event_date), event_time, movie_id)
) WITH CLUSTERING ORDER BY (event_time DESC, movie_id ASC);

CREATE TABLE IF NOT EXISTS movie_search_event (
  user_md5 text,
  event_date date,
  event_time timeuuid,
  keyword text,
  movie_id bigint,
  PRIMARY KEY ((user_md5, event_date), event_time, keyword)
) WITH CLUSTERING ORDER BY (event_time DESC, keyword ASC);

CREATE TABLE IF NOT EXISTS favorite_event (
  user_md5 text,
  event_date date,
  event_time timeuuid,
  movie_id bigint,
  event_type text,
  PRIMARY KEY ((user_md5, event_date), event_time, movie_id)
) WITH CLUSTERING ORDER BY (event_time DESC, movie_id ASC);

CREATE TABLE IF NOT EXISTS recommend_exposure_event (
  user_md5 text,
  event_date date,
  event_time timeuuid,
  movie_id bigint,
  scene text,
  PRIMARY KEY ((user_md5, event_date), event_time, movie_id)
) WITH CLUSTERING ORDER BY (event_time DESC, movie_id ASC);

CREATE TABLE IF NOT EXISTS recommend_click_event (
  user_md5 text,
  event_date date,
  event_time timeuuid,
  movie_id bigint,
  scene text,
  PRIMARY KEY ((user_md5, event_date), event_time, movie_id)
) WITH CLUSTERING ORDER BY (event_time DESC, movie_id ASC);

CREATE TABLE IF NOT EXISTS rating_event (
  rating_id bigint,
  user_md5 text,
  event_date date,
  event_time timeuuid,
  movie_id bigint,
  rating int,
  PRIMARY KEY ((user_md5, event_date), event_time, movie_id)
) WITH CLUSTERING ORDER BY (event_time DESC, movie_id ASC);

CREATE TABLE IF NOT EXISTS user_access_timeline (
  user_md5 text,
  event_date date,
  event_time timeuuid,
  event_type text,
  movie_id bigint,
  PRIMARY KEY ((user_md5, event_date), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);

TRUNCATE movie_browse_event;
TRUNCATE movie_click_event;
TRUNCATE movie_search_event;
TRUNCATE favorite_event;
TRUNCATE recommend_exposure_event;
TRUNCATE recommend_click_event;
TRUNCATE rating_event;
TRUNCATE user_access_timeline;

INSERT INTO movie_browse_event (user_md5, event_date, event_time, movie_id, region_code, device_type) VALUES ('0ab7e3efacd56983f16503572d2b9915', '2026-03-23', now(), 35267208, 'CN-BJ', 'iOS');
INSERT INTO movie_browse_event (user_md5, event_date, event_time, movie_id, region_code, device_type) VALUES ('5f4dcc3b5aa765d61d8327deb882cf99', '2026-03-23', now(), 36779979, 'CN-SH', 'Android');
INSERT INTO movie_browse_event (user_md5, event_date, event_time, movie_id, region_code, device_type) VALUES ('9e107d9d372bb6826bd81d3542a419d6', '2026-03-23', now(), 90000001, 'CN-CD', 'Web');
INSERT INTO movie_browse_event (user_md5, event_date, event_time, movie_id, region_code, device_type) VALUES ('e10adc3949ba59abbe56e057f20f883e', '2026-03-23', now(), 90000002, 'CN-GZ', 'Android');
INSERT INTO movie_browse_event (user_md5, event_date, event_time, movie_id, region_code, device_type) VALUES ('25d55ad283aa400af464c76d713c07ad', '2026-03-23', now(), 90000003, 'CN-HZ', 'iOS');

INSERT INTO movie_click_event (user_md5, event_date, event_time, movie_id, source) VALUES ('0ab7e3efacd56983f16503572d2b9915', '2026-03-23', now(), 35267208, 'home_feed');
INSERT INTO movie_click_event (user_md5, event_date, event_time, movie_id, source) VALUES ('5f4dcc3b5aa765d61d8327deb882cf99', '2026-03-23', now(), 36779979, 'search_result');
INSERT INTO movie_click_event (user_md5, event_date, event_time, movie_id, source) VALUES ('9e107d9d372bb6826bd81d3542a419d6', '2026-03-23', now(), 90000001, 'classic_channel');
INSERT INTO movie_click_event (user_md5, event_date, event_time, movie_id, source) VALUES ('e10adc3949ba59abbe56e057f20f883e', '2026-03-23', now(), 90000002, 'sci_fi_tab');
INSERT INTO movie_click_event (user_md5, event_date, event_time, movie_id, source) VALUES ('25d55ad283aa400af464c76d713c07ad', '2026-03-23', now(), 25845392, 'war_topic');

INSERT INTO movie_search_event (user_md5, event_date, event_time, keyword, movie_id) VALUES ('0ab7e3efacd56983f16503572d2b9915', '2026-03-23', now(), '科幻', 35267208);
INSERT INTO movie_search_event (user_md5, event_date, event_time, keyword, movie_id) VALUES ('5f4dcc3b5aa765d61d8327deb882cf99', '2026-03-23', now(), '热辣滚烫', 36779979);
INSERT INTO movie_search_event (user_md5, event_date, event_time, keyword, movie_id) VALUES ('9e107d9d372bb6826bd81d3542a419d6', '2026-03-23', now(), '奥本海默', 90000001);
INSERT INTO movie_search_event (user_md5, event_date, event_time, keyword, movie_id) VALUES ('e10adc3949ba59abbe56e057f20f883e', '2026-03-23', now(), '沙丘2', 90000002);
INSERT INTO movie_search_event (user_md5, event_date, event_time, keyword, movie_id) VALUES ('c33367701511b4f6020ec61ded352059', '2026-03-23', now(), '壮志凌云2', 90000004);

INSERT INTO favorite_event (user_md5, event_date, event_time, movie_id, event_type) VALUES ('0ab7e3efacd56983f16503572d2b9915', '2026-03-23', now(), 35267208, 'ADD');
INSERT INTO favorite_event (user_md5, event_date, event_time, movie_id, event_type) VALUES ('5f4dcc3b5aa765d61d8327deb882cf99', '2026-03-23', now(), 36779979, 'ADD');
INSERT INTO favorite_event (user_md5, event_date, event_time, movie_id, event_type) VALUES ('9e107d9d372bb6826bd81d3542a419d6', '2026-03-23', now(), 90000001, 'ADD');
INSERT INTO favorite_event (user_md5, event_date, event_time, movie_id, event_type) VALUES ('96e79218965eb72c92a549dd5a330112', '2026-03-23', now(), 30474725, 'CANCEL');

INSERT INTO recommend_exposure_event (user_md5, event_date, event_time, movie_id, scene) VALUES ('0ab7e3efacd56983f16503572d2b9915', '2026-03-23', now(), 35267208, 'home_feed');
INSERT INTO recommend_exposure_event (user_md5, event_date, event_time, movie_id, scene) VALUES ('0ab7e3efacd56983f16503572d2b9915', '2026-03-23', now(), 90000002, 'home_feed');
INSERT INTO recommend_exposure_event (user_md5, event_date, event_time, movie_id, scene) VALUES ('5f4dcc3b5aa765d61d8327deb882cf99', '2026-03-23', now(), 36779979, 'detail_page');
INSERT INTO recommend_exposure_event (user_md5, event_date, event_time, movie_id, scene) VALUES ('9e107d9d372bb6826bd81d3542a419d6', '2026-03-23', now(), 90000001, 'global_classic');
INSERT INTO recommend_exposure_event (user_md5, event_date, event_time, movie_id, scene) VALUES ('e10adc3949ba59abbe56e057f20f883e', '2026-03-23', now(), 90000002, 'sci_fi_tab');

INSERT INTO recommend_click_event (user_md5, event_date, event_time, movie_id, scene) VALUES ('0ab7e3efacd56983f16503572d2b9915', '2026-03-23', now(), 35267208, 'home_feed');
INSERT INTO recommend_click_event (user_md5, event_date, event_time, movie_id, scene) VALUES ('5f4dcc3b5aa765d61d8327deb882cf99', '2026-03-23', now(), 36779979, 'detail_page');
INSERT INTO recommend_click_event (user_md5, event_date, event_time, movie_id, scene) VALUES ('9e107d9d372bb6826bd81d3542a419d6', '2026-03-23', now(), 90000001, 'global_classic');
INSERT INTO recommend_click_event (user_md5, event_date, event_time, movie_id, scene) VALUES ('d8578edf8458ce06fbc5bb76a58c5ca4', '2026-03-23', now(), 90000003, 'home_feed');

INSERT INTO rating_event (rating_id, user_md5, event_date, event_time, movie_id, rating) VALUES (920000001, '0ab7e3efacd56983f16503572d2b9915', '2026-03-23', now(), 35267208, 5);
INSERT INTO rating_event (rating_id, user_md5, event_date, event_time, movie_id, rating) VALUES (920000002, '5f4dcc3b5aa765d61d8327deb882cf99', '2026-03-23', now(), 36779979, 4);
INSERT INTO rating_event (rating_id, user_md5, event_date, event_time, movie_id, rating) VALUES (920000003, '9e107d9d372bb6826bd81d3542a419d6', '2026-03-23', now(), 90000001, 5);
INSERT INTO rating_event (rating_id, user_md5, event_date, event_time, movie_id, rating) VALUES (920000004, 'e10adc3949ba59abbe56e057f20f883e', '2026-03-23', now(), 90000002, 5);
INSERT INTO rating_event (rating_id, user_md5, event_date, event_time, movie_id, rating) VALUES (920000005, '25d55ad283aa400af464c76d713c07ad', '2026-03-23', now(), 25845392, 4);

INSERT INTO user_access_timeline (user_md5, event_date, event_time, event_type, movie_id) VALUES ('0ab7e3efacd56983f16503572d2b9915', '2026-03-23', now(), 'view_detail', 35267208);
INSERT INTO user_access_timeline (user_md5, event_date, event_time, event_type, movie_id) VALUES ('5f4dcc3b5aa765d61d8327deb882cf99', '2026-03-23', now(), 'search', 36779979);
INSERT INTO user_access_timeline (user_md5, event_date, event_time, event_type, movie_id) VALUES ('9e107d9d372bb6826bd81d3542a419d6', '2026-03-23', now(), 'post_review', 90000001);
INSERT INTO user_access_timeline (user_md5, event_date, event_time, event_type, movie_id) VALUES ('e10adc3949ba59abbe56e057f20f883e', '2026-03-23', now(), 'share_movie', 90000002);
INSERT INTO user_access_timeline (user_md5, event_date, event_time, event_type, movie_id) VALUES ('c33367701511b4f6020ec61ded352059', '2026-03-23', now(), 'search', 90000004);
