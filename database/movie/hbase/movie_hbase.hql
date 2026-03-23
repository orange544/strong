# Run in HBase shell.
# create_namespace 'movie_ns'

# rowkey: movie_id
create 'movie_ns:movie_meta_wide',
  { NAME => 'basic', VERSIONS => 3 },
  { NAME => 'score', VERSIONS => 3 },
  { NAME => 'id', VERSIONS => 3 },
  { NAME => 'rel', VERSIONS => 3 },
  { NAME => 'text', VERSIONS => 3 }

# rowkey: movie_id#version_time
create 'movie_ns:movie_version_archive',
  { NAME => 'meta', VERSIONS => 20 },
  { NAME => 'audit', VERSIONS => 5 }

# rowkey: person_id
create 'movie_ns:person_ext_profile',
  { NAME => 'basic', VERSIONS => 5 },
  { NAME => 'text', VERSIONS => 5 }

# rowkey: movie_id
create 'movie_ns:movie_id_mapping',
  { NAME => 'map', VERSIONS => 5 },
  { NAME => 'trace', VERSIONS => 5 }

# rowkey: user_md5#event_time
create 'movie_ns:behavior_archive',
  { NAME => 'event', VERSIONS => 3 },
  { NAME => 'metric', VERSIONS => 3 }

# rowkey: movie_id#crawl_time
create 'movie_ns:crawler_movie_raw',
  { NAME => 'raw', VERSIONS => 3 },
  { NAME => 'clean', VERSIONS => 3 }

# rowkey: keyword#stat_date
create 'movie_ns:search_index_mid',
  { NAME => 'stat', VERSIONS => 3 },
  { NAME => 'map', VERSIONS => 3 }

# rowkey: movie_id
create 'movie_ns:movie_tag_wide',
  { NAME => 'tag', VERSIONS => 5 },
  { NAME => 'stat', VERSIONS => 5 }

# ------------------------------------------------------------------
# Seed data for enterprise-like wide/archival scenarios.
# ------------------------------------------------------------------
put 'movie_ns:movie_meta_wide', '35267208', 'basic:name', '流浪地球2'
put 'movie_ns:movie_meta_wide', '35267208', 'basic:name_en', 'The Wandering Earth II'
put 'movie_ns:movie_meta_wide', '35267208', 'score:douban_score', '8.3'
put 'movie_ns:movie_meta_wide', '35267208', 'id:imdb_id', 'tt13539646'
put 'movie_ns:movie_meta_wide', '35267208', 'rel:country', 'CN'
put 'movie_ns:movie_meta_wide', '35267208', 'text:tags', '春节档,硬科幻,近五年经典'

put 'movie_ns:movie_meta_wide', '90000001', 'basic:name', '奥本海默'
put 'movie_ns:movie_meta_wide', '90000001', 'basic:name_en', 'Oppenheimer'
put 'movie_ns:movie_meta_wide', '90000001', 'score:douban_score', '8.8'
put 'movie_ns:movie_meta_wide', '90000001', 'id:imdb_id', 'tt15398776'
put 'movie_ns:movie_meta_wide', '90000001', 'rel:country', 'US,UK'
put 'movie_ns:movie_meta_wide', '90000001', 'text:tags', '传记,历史,奥斯卡'

put 'movie_ns:movie_version_archive', '35267208#2026-03-23T19:20:00', 'meta:version', 'v2026.03.23'
put 'movie_ns:movie_version_archive', '35267208#2026-03-23T19:20:00', 'meta:operator', 'mdm_sync_job'
put 'movie_ns:movie_version_archive', '35267208#2026-03-23T19:20:00', 'audit:action', 'UPSERT'
put 'movie_ns:movie_version_archive', '35267208#2026-03-23T19:20:00', 'audit:source', 'movie_master'

put 'movie_ns:person_ext_profile', '1000525', 'basic:name', '吴京'
put 'movie_ns:person_ext_profile', '1000525', 'basic:profession', '演员'
put 'movie_ns:person_ext_profile', '1000525', 'text:bio', '中国电影演员，主演多部商业类型片。'

put 'movie_ns:person_ext_profile', '9001101', 'basic:name', '基里安·墨菲'
put 'movie_ns:person_ext_profile', '9001101', 'basic:profession', '演员'
put 'movie_ns:person_ext_profile', '9001101', 'text:bio', '爱尔兰演员，代表作包括奥本海默。'

put 'movie_ns:movie_id_mapping', '35267208', 'map:douban_movie_id', '35267208'
put 'movie_ns:movie_id_mapping', '35267208', 'map:imdb_id', 'tt13539646'
put 'movie_ns:movie_id_mapping', '35267208', 'trace:last_sync', '2026-03-23T19:20:00+08:00'
put 'movie_ns:movie_id_mapping', '90000001', 'map:internal_movie_id', '90000001'
put 'movie_ns:movie_id_mapping', '90000001', 'map:imdb_id', 'tt15398776'
put 'movie_ns:movie_id_mapping', '90000001', 'trace:last_sync', '2026-03-23T19:20:00+08:00'

put 'movie_ns:behavior_archive', '0ab7e3efacd56983f16503572d2b9915#2026-03-23T19:05:32', 'event:type', 'recommend_click'
put 'movie_ns:behavior_archive', '0ab7e3efacd56983f16503572d2b9915#2026-03-23T19:05:32', 'event:movie_id', '35267208'
put 'movie_ns:behavior_archive', '0ab7e3efacd56983f16503572d2b9915#2026-03-23T19:05:32', 'metric:stay_seconds', '95'
put 'movie_ns:behavior_archive', '5f4dcc3b5aa765d61d8327deb882cf99#2026-03-23T19:12:08', 'event:type', 'search'
put 'movie_ns:behavior_archive', '5f4dcc3b5aa765d61d8327deb882cf99#2026-03-23T19:12:08', 'event:keyword', '热辣滚烫'
put 'movie_ns:behavior_archive', '5f4dcc3b5aa765d61d8327deb882cf99#2026-03-23T19:12:08', 'metric:result_count', '1'

put 'movie_ns:crawler_movie_raw', '35267208#2026-03-23T02:00:00', 'raw:source', 'douban'
put 'movie_ns:crawler_movie_raw', '35267208#2026-03-23T02:00:00', 'raw:payload', '{"name":"流浪地球2","score":8.3}'
put 'movie_ns:crawler_movie_raw', '35267208#2026-03-23T02:00:00', 'clean:normalized_name', '流浪地球2'
put 'movie_ns:crawler_movie_raw', '35267208#2026-03-23T02:00:00', 'clean:normalized_score', '8.3'

put 'movie_ns:search_index_mid', '科幻#2026-03-23', 'stat:search_count', '15820'
put 'movie_ns:search_index_mid', '科幻#2026-03-23', 'map:movie_ids', '35267208,90000002,90000003'
put 'movie_ns:search_index_mid', '春节档#2026-03-23', 'stat:search_count', '8090'
put 'movie_ns:search_index_mid', '春节档#2026-03-23', 'map:movie_ids', '35267208,36779979,30474725,34841067'

put 'movie_ns:movie_tag_wide', '36779979', 'tag:genre', '剧情,喜剧,运动'
put 'movie_ns:movie_tag_wide', '36779979', 'tag:keywords', '拳击,女性成长,春节档'
put 'movie_ns:movie_tag_wide', '36779979', 'stat:hot_score', '97.6'
put 'movie_ns:movie_tag_wide', '90000004', 'tag:genre', '动作,剧情'
put 'movie_ns:movie_tag_wide', '90000004', 'tag:keywords', '飞行,战机,视听'
put 'movie_ns:movie_tag_wide', '90000004', 'stat:hot_score', '93.2'
