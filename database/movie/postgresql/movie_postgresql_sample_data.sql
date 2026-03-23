SET search_path TO movie;

TRUNCATE TABLE
  field_semantic_dictionary,
  movie_language_mapping,
  movie_country_mapping,
  movie_genre_mapping,
  movie_director_mapping,
  movie_actor_mapping,
  movie_release_version,
  movie_alias_mapping,
  movie_standard_id,
  movie_master,
  director_master,
  actor_master,
  language_dictionary,
  country_dictionary,
  genre_dictionary
RESTART IDENTITY CASCADE;

INSERT INTO genre_dictionary (genre_code, genre_name_zh, genre_name_en, genre_group) VALUES
('GEN_SCI_FI', '科幻', 'Science Fiction', '故事片'),
('GEN_ADVENTURE', '冒险', 'Adventure', '故事片'),
('GEN_DRAMA', '剧情', 'Drama', '故事片'),
('GEN_COMEDY', '喜剧', 'Comedy', '故事片'),
('GEN_WAR', '战争', 'War', '故事片'),
('GEN_HISTORY', '历史', 'History', '故事片'),
('GEN_ACTION', '动作', 'Action', '故事片'),
('GEN_FANTASY', '奇幻', 'Fantasy', '故事片'),
('GEN_MYSTERY', '悬疑', 'Mystery', '故事片'),
('GEN_BIOGRAPHY', '传记', 'Biography', '故事片'),
('GEN_SPORT', '运动', 'Sport', '故事片');

INSERT INTO country_dictionary (country_code, country_name_zh, country_name_en, region_group) VALUES
('CN', '中国大陆', 'China Mainland', 'APAC'),
('HK', '中国香港', 'Hong Kong, China', 'APAC'),
('US', '美国', 'United States', 'NA'),
('UK', '英国', 'United Kingdom', 'EU'),
('CA', '加拿大', 'Canada', 'NA');

INSERT INTO language_dictionary (language_code, language_name_zh, language_name_en, iso_639_1) VALUES
('ZH_CN', '汉语普通话', 'Mandarin Chinese', 'zh'),
('EN', '英语', 'English', 'en');

INSERT INTO actor_master (actor_id, standard_name_zh, standard_name_en, sex, profession) VALUES
(1000525, '吴京', 'Jing Wu', '男', '演员'),
(1054455, '刘德华', 'Andy Lau', '男', '演员'),
(1274841, '李雪健', NULL, '男', '演员'),
(1274384, '沙溢', NULL, '男', '演员'),
(1276155, '贾玲', 'Ling Jia', '女', '演员'),
(1274239, '雷佳音', NULL, '男', '演员'),
(1316761, '张小斐', NULL, '女', '演员'),
(1325123, '易烊千玺', 'Jackson Yee', '男', '演员'),
(1274774, '段奕宏', NULL, '男', '演员'),
(1274238, '朱亚文', NULL, '男', '演员'),
(1274259, '沈腾', NULL, '男', '演员'),
(1274761, '张译', NULL, '男', '演员'),
(1041022, '费翔', 'Kris Phillips', '男', '演员'),
(1274242, '黄渤', NULL, '男', '演员'),
(1463599, '于适', NULL, '男', '演员'),
(1315500, '陈赫', NULL, '男', '演员'),
(9001101, '基里安·墨菲', 'Cillian Murphy', '男', '演员'),
(9001102, '艾米莉·布朗特', 'Emily Blunt', '女', '演员'),
(9001103, '小罗伯特·唐尼', 'Robert Downey Jr.', '男', '演员'),
(9001105, '提莫西·查拉梅', 'Timothee Chalamet', '男', '演员'),
(9001106, '赞达亚', 'Zendaya', '女', '演员'),
(9001107, '丽贝卡·弗格森', 'Rebecca Ferguson', '女', '演员'),
(9001109, '汤姆·赫兰德', 'Tom Holland', '男', '演员'),
(9001110, '本尼迪克特·康伯巴奇', 'Benedict Cumberbatch', '男', '演员'),
(9001112, '汤姆·克鲁斯', 'Tom Cruise', '男', '演员'),
(9001113, '迈尔斯·特勒', 'Miles Teller', '男', '演员');

INSERT INTO director_master (director_id, standard_name_zh, standard_name_en, sex, profession) VALUES
(1348151, '郭帆', 'Frant Gwo', '男', '导演'),
(1276155, '贾玲', 'Ling Jia', '女', '导演'),
(9002101, '克里斯托弗·诺兰', 'Christopher Nolan', '男', '导演'),
(9002102, '丹尼斯·维伦纽瓦', 'Denis Villeneuve', '男', '导演'),
(1023040, '陈凯歌', 'Kaige Chen', '男', '导演'),
(1274307, '徐克', 'Hark Tsui', '男', '导演'),
(1274275, '林超贤', 'Dante Lam', '男', '导演'),
(9002103, '乔·沃茨', 'Jon Watts', '男', '导演'),
(1054428, '张艺谋', 'Yimou Zhang', '男', '导演'),
(1316699, '乌尔善', 'Wuershan', '男', '导演'),
(9002104, '约瑟夫·科辛斯基', 'Joseph Kosinski', '男', '导演');

INSERT INTO movie_master (
  movie_id, standard_movie_code, standard_name_zh, standard_name_en, original_name,
  release_date, release_year, duration_minutes, imdb_id, douban_score, douban_votes,
  primary_genre_code, primary_country_code, primary_language_code, storyline
) VALUES
(35267208, 'MOV-CN-2023-0001', '流浪地球2', 'The Wandering Earth II', '流浪地球2', '2023-01-22', 2023, 173, 'tt13539646', 8.3, 1325089, 'GEN_SCI_FI', 'CN', 'ZH_CN', '地球危机升级，数字生命与移山计划并行。'),
(36779979, 'MOV-CN-2024-0002', '热辣滚烫', 'YOLO', '热辣滚烫', '2024-02-10', 2024, 129, 'tt30953582', 7.8, 864122, 'GEN_SPORT', 'CN', 'ZH_CN', '主角通过拳击与生活对抗，完成自我成长。'),
(90000001, 'MOV-US-2023-0003', '奥本海默', 'Oppenheimer', 'Oppenheimer', '2023-08-30', 2023, 180, 'tt15398776', 8.8, 1054321, 'GEN_BIOGRAPHY', 'US', 'EN', '围绕原子弹研发过程展开的历史传记电影。'),
(90000002, 'MOV-US-2024-0004', '沙丘2', 'Dune: Part Two', 'Dune: Part Two', '2024-03-08', 2024, 166, 'tt15239678', 8.5, 702113, 'GEN_SCI_FI', 'US', 'EN', '保罗在厄拉科斯崛起并走向预言命运。'),
(25845392, 'MOV-CN-2021-0005', '长津湖', 'The Battle at Lake Changjin', '长津湖', '2021-09-30', 2021, 176, 'tt13462900', 7.4, 926541, 'GEN_WAR', 'CN', 'ZH_CN', '抗美援朝关键战役的群像叙事。'),
(90000003, 'MOV-US-2021-0006', '蜘蛛侠：英雄无归', 'Spider-Man: No Way Home', 'Spider-Man: No Way Home', '2021-12-17', 2021, 148, 'tt10872600', 8.1, 812334, 'GEN_ACTION', 'US', 'EN', '身份危机引发多元宇宙连锁反应。'),
(30474725, 'MOV-CN-2023-0007', '满江红', 'Full River Red', '满江红', '2023-01-22', 2023, 159, 'tt21181940', 7.0, 1214021, 'GEN_MYSTERY', 'CN', 'ZH_CN', '南宋背景下的密信追查与反转叙事。'),
(34780991, 'MOV-CN-2023-0008', '封神第一部：朝歌风云', 'Creation of the Gods I', '封神第一部：朝歌风云', '2023-07-20', 2023, 148, 'tt6979756', 7.7, 1025683, 'GEN_FANTASY', 'CN', 'ZH_CN', '神话史诗世界观的开篇。'),
(90000004, 'MOV-US-2022-0009', '壮志凌云2：独行侠', 'Top Gun: Maverick', 'Top Gun: Maverick', '2022-05-27', 2022, 131, 'tt1745960', 8.7, 653402, 'GEN_ACTION', 'US', 'EN', '王牌飞行员重返训练体系执行高风险任务。'),
(34841067, 'MOV-CN-2021-0010', '你好，李焕英', 'Hi, Mom', '你好，李焕英', '2021-02-12', 2021, 128, 'tt14188440', 7.7, 1456702, 'GEN_COMEDY', 'CN', 'ZH_CN', '亲情主题下的穿越喜剧。');

INSERT INTO movie_standard_id (source_system, source_object, source_id, movie_id, standard_movie_code, source_name, is_primary) VALUES
('douban', 'movie', '35267208', 35267208, 'MOV-CN-2023-0001', '豆瓣电影', TRUE),
('imdb', 'movie', 'tt13539646', 35267208, 'MOV-CN-2023-0001', 'IMDb', FALSE),
('douban', 'movie', '36779979', 36779979, 'MOV-CN-2024-0002', '豆瓣电影', TRUE),
('imdb', 'movie', 'tt30953582', 36779979, 'MOV-CN-2024-0002', 'IMDb', FALSE),
('douban', 'movie', '90000001', 90000001, 'MOV-US-2023-0003', '豆瓣电影', TRUE),
('imdb', 'movie', 'tt15398776', 90000001, 'MOV-US-2023-0003', 'IMDb', FALSE),
('douban', 'movie', '90000002', 90000002, 'MOV-US-2024-0004', '豆瓣电影', TRUE),
('imdb', 'movie', 'tt15239678', 90000002, 'MOV-US-2024-0004', 'IMDb', FALSE),
('douban', 'movie', '25845392', 25845392, 'MOV-CN-2021-0005', '豆瓣电影', TRUE),
('imdb', 'movie', 'tt13462900', 25845392, 'MOV-CN-2021-0005', 'IMDb', FALSE),
('douban', 'movie', '90000003', 90000003, 'MOV-US-2021-0006', '豆瓣电影', TRUE),
('imdb', 'movie', 'tt10872600', 90000003, 'MOV-US-2021-0006', 'IMDb', FALSE),
('douban', 'movie', '30474725', 30474725, 'MOV-CN-2023-0007', '豆瓣电影', TRUE),
('imdb', 'movie', 'tt21181940', 30474725, 'MOV-CN-2023-0007', 'IMDb', FALSE),
('douban', 'movie', '34780991', 34780991, 'MOV-CN-2023-0008', '豆瓣电影', TRUE),
('imdb', 'movie', 'tt6979756', 34780991, 'MOV-CN-2023-0008', 'IMDb', FALSE),
('douban', 'movie', '90000004', 90000004, 'MOV-US-2022-0009', '豆瓣电影', TRUE),
('imdb', 'movie', 'tt1745960', 90000004, 'MOV-US-2022-0009', 'IMDb', FALSE),
('douban', 'movie', '34841067', 34841067, 'MOV-CN-2021-0010', '豆瓣电影', TRUE),
('imdb', 'movie', 'tt14188440', 34841067, 'MOV-CN-2021-0010', 'IMDb', FALSE);

INSERT INTO movie_alias_mapping (movie_id, alias_name, alias_language_code, alias_type, source_system) VALUES
(35267208, 'The Wandering Earth II', 'EN', 'ENGLISH_TITLE', 'douban_dataset'),
(36779979, 'YOLO', 'EN', 'ENGLISH_TITLE', 'douban_dataset'),
(90000001, 'Oppenheimer', 'EN', 'ORIGINAL_TITLE', 'imdb'),
(90000002, 'Dune: Part Two', 'EN', 'ORIGINAL_TITLE', 'imdb'),
(25845392, 'The Battle at Lake Changjin', 'EN', 'ENGLISH_TITLE', 'douban_dataset'),
(90000003, 'Spider-Man: No Way Home', 'EN', 'ORIGINAL_TITLE', 'imdb'),
(30474725, 'Full River Red', 'EN', 'ENGLISH_TITLE', 'douban_dataset'),
(34780991, 'Creation of the Gods I', 'EN', 'ENGLISH_TITLE', 'douban_dataset'),
(90000004, 'Top Gun: Maverick', 'EN', 'ORIGINAL_TITLE', 'imdb'),
(34841067, 'Hi, Mom', 'EN', 'ENGLISH_TITLE', 'douban_dataset');

INSERT INTO movie_release_version (movie_id, version_name, region_code, language_code, release_date, duration_minutes, is_re_release, source_system) VALUES
(35267208, 'IMAX 2D', 'CN', 'ZH_CN', '2023-01-22', 173, FALSE, 'ticketing_system'),
(36779979, '2D', 'CN', 'ZH_CN', '2024-02-10', 129, FALSE, 'ticketing_system'),
(90000001, 'IMAX 2D', 'US', 'EN', '2023-08-30', 180, FALSE, 'global_distribution'),
(90000002, 'IMAX 2D', 'US', 'EN', '2024-03-08', 166, FALSE, 'global_distribution'),
(25845392, '2D', 'CN', 'ZH_CN', '2021-09-30', 176, FALSE, 'ticketing_system'),
(90000003, 'IMAX 3D', 'US', 'EN', '2021-12-17', 148, FALSE, 'global_distribution'),
(30474725, '4K', 'CN', 'ZH_CN', '2023-01-22', 159, FALSE, 'ticketing_system'),
(34780991, 'IMAX 2D', 'CN', 'ZH_CN', '2023-07-20', 148, FALSE, 'ticketing_system'),
(90000004, '2D', 'US', 'EN', '2022-05-27', 131, FALSE, 'global_distribution'),
(34841067, '2D', 'CN', 'ZH_CN', '2021-02-12', 128, FALSE, 'ticketing_system');

INSERT INTO movie_actor_mapping (movie_id, actor_id, cast_order, is_lead) VALUES
(35267208, 1000525, 1, TRUE), (35267208, 1054455, 2, TRUE),
(36779979, 1276155, 1, TRUE), (36779979, 1274239, 2, TRUE),
(90000001, 9001101, 1, TRUE), (90000001, 9001102, 2, TRUE),
(90000002, 9001105, 1, TRUE), (90000002, 9001106, 2, TRUE),
(25845392, 1000525, 1, TRUE), (25845392, 1325123, 2, TRUE),
(90000003, 9001109, 1, TRUE), (90000003, 9001110, 2, TRUE),
(30474725, 1274259, 1, TRUE), (30474725, 1274761, 2, TRUE),
(34780991, 1041022, 1, TRUE), (34780991, 1274242, 2, TRUE),
(90000004, 9001112, 1, TRUE), (90000004, 9001113, 2, TRUE),
(34841067, 1276155, 1, TRUE), (34841067, 1316761, 2, TRUE);

INSERT INTO movie_director_mapping (movie_id, director_id, director_order) VALUES
(35267208, 1348151, 1),
(36779979, 1276155, 1),
(90000001, 9002101, 1),
(90000002, 9002102, 1),
(25845392, 1023040, 1), (25845392, 1274307, 2), (25845392, 1274275, 3),
(90000003, 9002103, 1),
(30474725, 1054428, 1),
(34780991, 1316699, 1),
(90000004, 9002104, 1),
(34841067, 1276155, 1);

INSERT INTO movie_genre_mapping (movie_id, genre_code, is_primary) VALUES
(35267208, 'GEN_SCI_FI', TRUE), (35267208, 'GEN_ADVENTURE', FALSE),
(36779979, 'GEN_SPORT', TRUE), (36779979, 'GEN_COMEDY', FALSE),
(90000001, 'GEN_BIOGRAPHY', TRUE), (90000001, 'GEN_HISTORY', FALSE),
(90000002, 'GEN_SCI_FI', TRUE), (90000002, 'GEN_ACTION', FALSE),
(25845392, 'GEN_WAR', TRUE), (25845392, 'GEN_HISTORY', FALSE),
(90000003, 'GEN_ACTION', TRUE), (90000003, 'GEN_FANTASY', FALSE),
(30474725, 'GEN_MYSTERY', TRUE), (30474725, 'GEN_COMEDY', FALSE),
(34780991, 'GEN_FANTASY', TRUE), (34780991, 'GEN_ACTION', FALSE),
(90000004, 'GEN_ACTION', TRUE), (90000004, 'GEN_DRAMA', FALSE),
(34841067, 'GEN_COMEDY', TRUE), (34841067, 'GEN_FANTASY', FALSE);

INSERT INTO movie_country_mapping (movie_id, country_code, is_primary) VALUES
(35267208, 'CN', TRUE),
(36779979, 'CN', TRUE),
(90000001, 'US', TRUE), (90000001, 'UK', FALSE),
(90000002, 'US', TRUE), (90000002, 'CA', FALSE),
(25845392, 'CN', TRUE), (25845392, 'HK', FALSE),
(90000003, 'US', TRUE),
(30474725, 'CN', TRUE),
(34780991, 'CN', TRUE),
(90000004, 'US', TRUE),
(34841067, 'CN', TRUE);

INSERT INTO movie_language_mapping (movie_id, language_code, is_primary) VALUES
(35267208, 'ZH_CN', TRUE),
(36779979, 'ZH_CN', TRUE),
(90000001, 'EN', TRUE),
(90000002, 'EN', TRUE),
(25845392, 'ZH_CN', TRUE),
(90000003, 'EN', TRUE),
(30474725, 'ZH_CN', TRUE),
(34780991, 'ZH_CN', TRUE),
(90000004, 'EN', TRUE),
(34841067, 'ZH_CN', TRUE);

INSERT INTO field_semantic_dictionary (
  semantic_name, semantic_description, canonical_type, canonical_format,
  source_system, source_object, source_field, example_value, is_primary
) VALUES
('movie_id', '电影主标识（统一主键）', 'BIGINT', 'numeric_id', 'douban', 'movie', 'MOVIE_ID', '35267208', TRUE),
('movie_id', '电影主标识（标准库）', 'BIGINT', 'numeric_id', 'postgresql_mdm', 'movie_master', 'movie_id', '35267208', TRUE),
('standard_movie_code', '电影标准编码', 'VARCHAR', 'MOV-CC-YYYY-NNNN', 'postgresql_mdm', 'movie_master', 'standard_movie_code', 'MOV-CN-2023-0001', TRUE),
('standard_name_zh', '电影标准中文名', 'VARCHAR', 'text_zh', 'postgresql_mdm', 'movie_master', 'standard_name_zh', '流浪地球2', TRUE),
('standard_name_en', '电影标准英文名', 'VARCHAR', 'text_en', 'postgresql_mdm', 'movie_master', 'standard_name_en', 'The Wandering Earth II', FALSE),
('imdb_id', 'IMDb 标识', 'VARCHAR', 'tt[0-9]+', 'imdb', 'movie', 'IMDB_ID', 'tt13539646', FALSE),
('release_date', '上映日期', 'DATE', 'yyyy-mm-dd', 'douban', 'movie', 'RELEASE_DATE', '2023-01-22', FALSE),
('duration_minutes', '片长（分钟）', 'INT', 'minutes', 'douban', 'movie', 'MINS', '173', FALSE),
('genre_code', '类型标准编码', 'VARCHAR', 'GEN_*', 'postgresql_mdm', 'genre_dictionary', 'genre_code', 'GEN_SCI_FI', TRUE),
('country_code', '国家/地区标准编码', 'VARCHAR', 'ISO_3166_like', 'postgresql_mdm', 'country_dictionary', 'country_code', 'CN', TRUE),
('language_code', '语言标准编码', 'VARCHAR', 'LANG_CODE', 'postgresql_mdm', 'language_dictionary', 'language_code', 'ZH_CN', TRUE),
('actor_id', '演员标准标识', 'BIGINT', 'numeric_id', 'douban', 'person', 'PERSON_ID', '1000525', TRUE),
('director_id', '导演标准标识', 'BIGINT', 'numeric_id', 'douban', 'person', 'PERSON_ID', '1348151', TRUE),
('alias_name', '电影别名或译名', 'VARCHAR', 'text', 'postgresql_mdm', 'movie_alias_mapping', 'alias_name', 'The Wandering Earth II', FALSE);
