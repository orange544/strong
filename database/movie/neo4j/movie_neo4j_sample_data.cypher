// Neo4j sample data for movie knowledge graph.
// Scope: sample data only (nodes + relationships), no changes to other databases.
// Shared keys with other systems: movie_id, person_id, user_md5, comment_id, rating_id.

MATCH (n) DETACH DELETE n;

UNWIND [
  {name:'科幻', name_en:'Science Fiction'},
  {name:'冒险', name_en:'Adventure'},
  {name:'剧情', name_en:'Drama'},
  {name:'喜剧', name_en:'Comedy'},
  {name:'运动', name_en:'Sport'},
  {name:'战争', name_en:'War'},
  {name:'历史', name_en:'History'},
  {name:'动作', name_en:'Action'},
  {name:'奇幻', name_en:'Fantasy'},
  {name:'悬疑', name_en:'Mystery'},
  {name:'传记', name_en:'Biography'}
] AS row
MERGE (g:Genre {name: row.name})
SET g.name_en = row.name_en,
    g.source_system = 'douban_dataset';

UNWIND [
  {name:'中国大陆', name_en:'China Mainland', country_code:'CN'},
  {name:'中国香港', name_en:'Hong Kong, China', country_code:'HK'},
  {name:'美国', name_en:'United States', country_code:'US'},
  {name:'英国', name_en:'United Kingdom', country_code:'UK'},
  {name:'加拿大', name_en:'Canada', country_code:'CA'}
] AS row
MERGE (c:Country {name: row.name})
SET c.name_en = row.name_en,
    c.country_code = row.country_code,
    c.source_system = 'standard_dictionary';

UNWIND [
  {company_id:'COMP_1001', name:'中国电影股份有限公司', name_en:'China Film Co., Ltd.'},
  {company_id:'COMP_1002', name:'上海儒意影视制作有限公司', name_en:'Ruyi Film'},
  {company_id:'COMP_1003', name:'北京光线影业有限公司', name_en:'Beijing Enlight Pictures'},
  {company_id:'COMP_1004', name:'环球影业', name_en:'Universal Pictures'},
  {company_id:'COMP_1005', name:'华纳兄弟影业', name_en:'Warner Bros. Pictures'},
  {company_id:'COMP_1006', name:'派拉蒙影业', name_en:'Paramount Pictures'}
] AS row
MERGE (co:Company {company_id: row.company_id})
SET co.name = row.name,
    co.name_en = row.name_en,
    co.source_system = 'mdm_company';

UNWIND [
  {award_id:'AWD_2023_ASIAN_POPULAR_FILM', name:'亚洲电影大奖 最受欢迎影片', name_en:'Asian Film Awards Most Popular Film', award_year:2023},
  {award_id:'AWD_2024_SPRING_TOP_BOXOFFICE', name:'春节档 票房年度冠军', name_en:'Spring Festival Top Box Office', award_year:2024},
  {award_id:'AWD_2024_OSCAR_BEST_PICTURE', name:'奥斯卡 最佳影片', name_en:'Oscar Best Picture', award_year:2024},
  {award_id:'AWD_2024_GLOBAL_IMAX', name:'全球 IMAX 热门影片', name_en:'Global IMAX Hit', award_year:2024},
  {award_id:'AWD_2022_GLOBAL_ACTION', name:'全球动作片年度口碑', name_en:'Global Action Film Recognition', award_year:2022}
] AS row
MERGE (a:Award {award_id: row.award_id})
SET a.name = row.name,
    a.name_en = row.name_en,
    a.award_year = row.award_year,
    a.source_system = 'award_archive';

UNWIND [
  {user_md5:'0ab7e3efacd56983f16503572d2b9915', user_nickname:'恋丶你灬'},
  {user_md5:'5f4dcc3b5aa765d61d8327deb882cf99', user_nickname:'风继续吹'},
  {user_md5:'9e107d9d372bb6826bd81d3542a419d6', user_nickname:'月落乌啼'},
  {user_md5:'e10adc3949ba59abbe56e057f20f883e', user_nickname:'银河补习班'},
  {user_md5:'25d55ad283aa400af464c76d713c07ad', user_nickname:'海边的曼彻斯特'},
  {user_md5:'d8578edf8458ce06fbc5bb76a58c5ca4', user_nickname:'花火'},
  {user_md5:'96e79218965eb72c92a549dd5a330112', user_nickname:'逆光飞行'},
  {user_md5:'6cb75f652a9b52798eb6cf2201057c73', user_nickname:'河畔青芒'},
  {user_md5:'c33367701511b4f6020ec61ded352059', user_nickname:'等风来'},
  {user_md5:'b59c67bf196a4758191e42f76670ceba', user_nickname:'南方有嘉木'}
] AS row
MERGE (u:User {user_md5: row.user_md5})
SET u.user_nickname = row.user_nickname,
    u.source_system = 'ticketing_system';

UNWIND [
  {
    movie_id:35267208,
    name:'流浪地球2',
    name_en:'The Wandering Earth II',
    alias:'The Wandering Earth II',
    imdb_id:'tt13539646',
    douban_score:8.3,
    douban_votes:1325089,
    release_date:'2023-01-22',
    year:2023,
    languages:['汉语普通话'],
    regions:['中国大陆']
  },
  {
    movie_id:36779979,
    name:'热辣滚烫',
    name_en:'YOLO',
    alias:'YOLO',
    imdb_id:'tt30953582',
    douban_score:7.8,
    douban_votes:864122,
    release_date:'2024-02-10',
    year:2024,
    languages:['汉语普通话'],
    regions:['中国大陆']
  },
  {
    movie_id:90000001,
    name:'奥本海默',
    name_en:'Oppenheimer',
    alias:'Oppenheimer',
    imdb_id:'tt15398776',
    douban_score:8.8,
    douban_votes:1054321,
    release_date:'2023-08-30',
    year:2023,
    languages:['英语'],
    regions:['美国','英国']
  },
  {
    movie_id:90000002,
    name:'沙丘2',
    name_en:'Dune: Part Two',
    alias:'Dune: Part Two',
    imdb_id:'tt15239678',
    douban_score:8.5,
    douban_votes:702113,
    release_date:'2024-03-08',
    year:2024,
    languages:['英语'],
    regions:['美国','加拿大']
  },
  {
    movie_id:25845392,
    name:'长津湖',
    name_en:'The Battle at Lake Changjin',
    alias:'长津湖之战',
    imdb_id:'tt13462900',
    douban_score:7.4,
    douban_votes:926541,
    release_date:'2021-09-30',
    year:2021,
    languages:['汉语普通话'],
    regions:['中国大陆','中国香港']
  },
  {
    movie_id:90000003,
    name:'蜘蛛侠：英雄无归',
    name_en:'Spider-Man: No Way Home',
    alias:'Spider-Man: No Way Home',
    imdb_id:'tt10872600',
    douban_score:8.1,
    douban_votes:812334,
    release_date:'2021-12-17',
    year:2021,
    languages:['英语'],
    regions:['美国']
  },
  {
    movie_id:30474725,
    name:'满江红',
    name_en:'Full River Red',
    alias:'满江红',
    imdb_id:'tt21181940',
    douban_score:7.0,
    douban_votes:1214021,
    release_date:'2023-01-22',
    year:2023,
    languages:['汉语普通话'],
    regions:['中国大陆']
  },
  {
    movie_id:34780991,
    name:'封神第一部：朝歌风云',
    name_en:'Creation of the Gods I',
    alias:'封神第一部',
    imdb_id:'tt6979756',
    douban_score:7.7,
    douban_votes:1025683,
    release_date:'2023-07-20',
    year:2023,
    languages:['汉语普通话'],
    regions:['中国大陆']
  },
  {
    movie_id:90000004,
    name:'壮志凌云2：独行侠',
    name_en:'Top Gun: Maverick',
    alias:'Top Gun: Maverick',
    imdb_id:'tt1745960',
    douban_score:8.7,
    douban_votes:653402,
    release_date:'2022-05-27',
    year:2022,
    languages:['英语'],
    regions:['美国']
  },
  {
    movie_id:34841067,
    name:'你好，李焕英',
    name_en:'Hi, Mom',
    alias:'Hi, Mom',
    imdb_id:'tt14188440',
    douban_score:7.7,
    douban_votes:1456702,
    release_date:'2021-02-12',
    year:2021,
    languages:['汉语普通话'],
    regions:['中国大陆']
  }
] AS row
MERGE (m:Movie {movie_id: row.movie_id})
SET m.name = row.name,
    m.name_en = row.name_en,
    m.alias = row.alias,
    m.imdb_id = row.imdb_id,
    m.douban_score = row.douban_score,
    m.douban_votes = row.douban_votes,
    m.release_date = row.release_date,
    m.year = row.year,
    m.languages = row.languages,
    m.regions = row.regions,
    m.source_system = 'douban_dataset';

UNWIND [
  {person_id:1000525, name:'吴京', name_en:'Jing Wu'},
  {person_id:1054455, name:'刘德华', name_en:'Andy Lau'},
  {person_id:1274841, name:'李雪健', name_en:null},
  {person_id:1274384, name:'沙溢', name_en:null},
  {person_id:1276155, name:'贾玲', name_en:'Ling Jia'},
  {person_id:1274239, name:'雷佳音', name_en:null},
  {person_id:1316761, name:'张小斐', name_en:null},
  {person_id:1325123, name:'易烊千玺', name_en:'Jackson Yee'},
  {person_id:1274774, name:'段奕宏', name_en:null},
  {person_id:1274238, name:'朱亚文', name_en:null},
  {person_id:1274259, name:'沈腾', name_en:null},
  {person_id:1274761, name:'张译', name_en:null},
  {person_id:1041022, name:'费翔', name_en:'Kris Phillips'},
  {person_id:1274242, name:'黄渤', name_en:null},
  {person_id:1463599, name:'于适', name_en:null},
  {person_id:1315500, name:'陈赫', name_en:null},
  {person_id:9001101, name:'基里安·墨菲', name_en:'Cillian Murphy'},
  {person_id:9001102, name:'艾米莉·布朗特', name_en:'Emily Blunt'},
  {person_id:9001103, name:'小罗伯特·唐尼', name_en:'Robert Downey Jr.'},
  {person_id:9001105, name:'提莫西·查拉梅', name_en:'Timothee Chalamet'},
  {person_id:9001106, name:'赞达亚', name_en:'Zendaya'},
  {person_id:9001107, name:'丽贝卡·弗格森', name_en:'Rebecca Ferguson'},
  {person_id:9001109, name:'汤姆·赫兰德', name_en:'Tom Holland'},
  {person_id:9001110, name:'本尼迪克特·康伯巴奇', name_en:'Benedict Cumberbatch'},
  {person_id:9001112, name:'汤姆·克鲁斯', name_en:'Tom Cruise'},
  {person_id:9001113, name:'迈尔斯·特勒', name_en:'Miles Teller'}
] AS row
MERGE (p:Person:Actor {person_id: row.person_id})
SET p.name = row.name,
    p.name_en = row.name_en,
    p.source_system = 'person_dataset';

UNWIND [
  {person_id:1348151, name:'郭帆', name_en:'Frant Gwo'},
  {person_id:1276155, name:'贾玲', name_en:'Ling Jia'},
  {person_id:9002101, name:'克里斯托弗·诺兰', name_en:'Christopher Nolan'},
  {person_id:9002102, name:'丹尼斯·维伦纽瓦', name_en:'Denis Villeneuve'},
  {person_id:1023040, name:'陈凯歌', name_en:'Kaige Chen'},
  {person_id:1274307, name:'徐克', name_en:'Hark Tsui'},
  {person_id:1274275, name:'林超贤', name_en:'Dante Lam'},
  {person_id:9002103, name:'乔·沃茨', name_en:'Jon Watts'},
  {person_id:1054428, name:'张艺谋', name_en:'Yimou Zhang'},
  {person_id:1316699, name:'乌尔善', name_en:'Wuershan'},
  {person_id:9002104, name:'约瑟夫·科辛斯基', name_en:'Joseph Kosinski'}
] AS row
MERGE (p:Person:Director {person_id: row.person_id})
SET p.name = row.name,
    p.name_en = row.name_en,
    p.source_system = 'person_dataset';

UNWIND [
  {movie_id:35267208, actor_id:1000525, cast_order:1},
  {movie_id:35267208, actor_id:1054455, cast_order:2},
  {movie_id:36779979, actor_id:1276155, cast_order:1},
  {movie_id:36779979, actor_id:1274239, cast_order:2},
  {movie_id:90000001, actor_id:9001101, cast_order:1},
  {movie_id:90000001, actor_id:9001102, cast_order:2},
  {movie_id:90000002, actor_id:9001105, cast_order:1},
  {movie_id:90000002, actor_id:9001106, cast_order:2},
  {movie_id:25845392, actor_id:1000525, cast_order:1},
  {movie_id:25845392, actor_id:1325123, cast_order:2},
  {movie_id:90000003, actor_id:9001109, cast_order:1},
  {movie_id:90000003, actor_id:9001110, cast_order:2},
  {movie_id:30474725, actor_id:1274259, cast_order:1},
  {movie_id:30474725, actor_id:1274761, cast_order:2},
  {movie_id:34780991, actor_id:1041022, cast_order:1},
  {movie_id:34780991, actor_id:1274242, cast_order:2},
  {movie_id:90000004, actor_id:9001112, cast_order:1},
  {movie_id:90000004, actor_id:9001113, cast_order:2},
  {movie_id:34841067, actor_id:1276155, cast_order:1},
  {movie_id:34841067, actor_id:1316761, cast_order:2}
] AS row
MATCH (a:Person:Actor {person_id: row.actor_id})
MATCH (m:Movie {movie_id: row.movie_id})
MERGE (a)-[r:ACTED_IN]->(m)
SET r.cast_order = row.cast_order,
    r.source_system = 'movie_master';

UNWIND [
  {movie_id:35267208, director_id:1348151, director_order:1},
  {movie_id:36779979, director_id:1276155, director_order:1},
  {movie_id:90000001, director_id:9002101, director_order:1},
  {movie_id:90000002, director_id:9002102, director_order:1},
  {movie_id:25845392, director_id:1023040, director_order:1},
  {movie_id:25845392, director_id:1274307, director_order:2},
  {movie_id:25845392, director_id:1274275, director_order:3},
  {movie_id:90000003, director_id:9002103, director_order:1},
  {movie_id:30474725, director_id:1054428, director_order:1},
  {movie_id:34780991, director_id:1316699, director_order:1},
  {movie_id:90000004, director_id:9002104, director_order:1},
  {movie_id:34841067, director_id:1276155, director_order:1}
] AS row
MATCH (m:Movie {movie_id: row.movie_id})
MATCH (d:Person:Director {person_id: row.director_id})
MERGE (m)-[r:DIRECTED_BY]->(d)
SET r.director_order = row.director_order,
    r.source_system = 'movie_master';

UNWIND [
  {movie_id:35267208, genre_name:'科幻'},
  {movie_id:35267208, genre_name:'冒险'},
  {movie_id:36779979, genre_name:'剧情'},
  {movie_id:36779979, genre_name:'喜剧'},
  {movie_id:36779979, genre_name:'运动'},
  {movie_id:90000001, genre_name:'剧情'},
  {movie_id:90000001, genre_name:'传记'},
  {movie_id:90000001, genre_name:'历史'},
  {movie_id:90000002, genre_name:'科幻'},
  {movie_id:90000002, genre_name:'动作'},
  {movie_id:90000002, genre_name:'冒险'},
  {movie_id:25845392, genre_name:'战争'},
  {movie_id:25845392, genre_name:'历史'},
  {movie_id:90000003, genre_name:'动作'},
  {movie_id:90000003, genre_name:'科幻'},
  {movie_id:90000003, genre_name:'奇幻'},
  {movie_id:30474725, genre_name:'悬疑'},
  {movie_id:30474725, genre_name:'喜剧'},
  {movie_id:34780991, genre_name:'奇幻'},
  {movie_id:34780991, genre_name:'动作'},
  {movie_id:90000004, genre_name:'动作'},
  {movie_id:90000004, genre_name:'剧情'},
  {movie_id:34841067, genre_name:'喜剧'},
  {movie_id:34841067, genre_name:'奇幻'}
] AS row
MATCH (m:Movie {movie_id: row.movie_id})
MATCH (g:Genre {name: row.genre_name})
MERGE (m)-[r:BELONGS_TO]->(g)
SET r.source_system = 'movie_master';

UNWIND [
  {movie_id:35267208, country_name:'中国大陆'},
  {movie_id:36779979, country_name:'中国大陆'},
  {movie_id:90000001, country_name:'美国'},
  {movie_id:90000001, country_name:'英国'},
  {movie_id:90000002, country_name:'美国'},
  {movie_id:90000002, country_name:'加拿大'},
  {movie_id:25845392, country_name:'中国大陆'},
  {movie_id:25845392, country_name:'中国香港'},
  {movie_id:90000003, country_name:'美国'},
  {movie_id:30474725, country_name:'中国大陆'},
  {movie_id:34780991, country_name:'中国大陆'},
  {movie_id:90000004, country_name:'美国'},
  {movie_id:34841067, country_name:'中国大陆'}
] AS row
MATCH (m:Movie {movie_id: row.movie_id})
MATCH (c:Country {name: row.country_name})
MERGE (m)-[r:RELEASED_IN]->(c)
SET r.source_system = 'movie_master';

UNWIND [
  {movie_id:35267208, company_id:'COMP_1001', company_role:'出品'},
  {movie_id:36779979, company_id:'COMP_1002', company_role:'出品'},
  {movie_id:90000001, company_id:'COMP_1005', company_role:'发行'},
  {movie_id:90000002, company_id:'COMP_1005', company_role:'发行'},
  {movie_id:25845392, company_id:'COMP_1001', company_role:'出品'},
  {movie_id:90000003, company_id:'COMP_1004', company_role:'发行'},
  {movie_id:30474725, company_id:'COMP_1002', company_role:'出品'},
  {movie_id:34780991, company_id:'COMP_1003', company_role:'联合出品'},
  {movie_id:90000004, company_id:'COMP_1006', company_role:'发行'},
  {movie_id:34841067, company_id:'COMP_1002', company_role:'出品'}
] AS row
MATCH (m:Movie {movie_id: row.movie_id})
MATCH (c:Company {company_id: row.company_id})
MERGE (m)-[r:PRODUCED_BY]->(c)
SET r.company_role = row.company_role,
    r.source_system = 'industry_registry';

UNWIND [
  {movie_id:35267208, award_id:'AWD_2023_ASIAN_POPULAR_FILM', category:'最受欢迎影片', award_year:2023},
  {movie_id:36779979, award_id:'AWD_2024_SPRING_TOP_BOXOFFICE', category:'票房冠军', award_year:2024},
  {movie_id:90000001, award_id:'AWD_2024_OSCAR_BEST_PICTURE', category:'最佳影片', award_year:2024},
  {movie_id:90000002, award_id:'AWD_2024_GLOBAL_IMAX', category:'IMAX 热门影片', award_year:2024},
  {movie_id:90000004, award_id:'AWD_2022_GLOBAL_ACTION', category:'动作片口碑奖', award_year:2022}
] AS row
MATCH (m:Movie {movie_id: row.movie_id})
MATCH (a:Award {award_id: row.award_id})
MERGE (m)-[r:WON_AWARD]->(a)
SET r.category = row.category,
    r.award_year = row.award_year,
    r.source_system = 'award_archive';

UNWIND [
  {user_md5:'0ab7e3efacd56983f16503572d2b9915', movie_id:35267208, rating_id:920000001, rating:5, rating_time:'2026-03-19 20:18:00'},
  {user_md5:'5f4dcc3b5aa765d61d8327deb882cf99', movie_id:36779979, rating_id:920000002, rating:4, rating_time:'2026-03-18 17:10:00'},
  {user_md5:'9e107d9d372bb6826bd81d3542a419d6', movie_id:90000001, rating_id:920000003, rating:5, rating_time:'2026-03-17 22:00:00'},
  {user_md5:'e10adc3949ba59abbe56e057f20f883e', movie_id:90000002, rating_id:920000004, rating:5, rating_time:'2026-03-20 10:35:00'},
  {user_md5:'25d55ad283aa400af464c76d713c07ad', movie_id:25845392, rating_id:920000005, rating:4, rating_time:'2026-03-19 13:15:00'},
  {user_md5:'d8578edf8458ce06fbc5bb76a58c5ca4', movie_id:90000003, rating_id:920000006, rating:5, rating_time:'2026-03-20 14:28:00'},
  {user_md5:'96e79218965eb72c92a549dd5a330112', movie_id:30474725, rating_id:920000007, rating:3, rating_time:'2026-03-20 16:10:00'},
  {user_md5:'6cb75f652a9b52798eb6cf2201057c73', movie_id:34780991, rating_id:920000008, rating:4, rating_time:'2026-03-20 17:58:00'},
  {user_md5:'c33367701511b4f6020ec61ded352059', movie_id:90000004, rating_id:920000009, rating:4, rating_time:'2026-03-20 19:53:00'},
  {user_md5:'b59c67bf196a4758191e42f76670ceba', movie_id:34841067, rating_id:920000010, rating:4, rating_time:'2026-03-20 21:06:00'}
] AS row
MATCH (u:User {user_md5: row.user_md5})
MATCH (m:Movie {movie_id: row.movie_id})
MERGE (u)-[r:RATED {rating_id: row.rating_id}]->(m)
SET r.rating = row.rating,
    r.rating_time = row.rating_time,
    r.source_system = 'rating_docs';

UNWIND [
  {user_md5:'0ab7e3efacd56983f16503572d2b9915', movie_id:35267208, comment_id:910000001, rating:5, votes:1280, comment_time:'2026-03-19 20:20:00', content:'工业质感比前作更稳，前段铺垫稍慢，后段情绪拉满。'},
  {user_md5:'5f4dcc3b5aa765d61d8327deb882cf99', movie_id:36779979, comment_id:910000002, rating:4, votes:860, comment_time:'2026-03-18 17:14:00', content:'拳击训练段落很扎实，后半段情绪爆发非常有感染力。'},
  {user_md5:'9e107d9d372bb6826bd81d3542a419d6', movie_id:90000001, comment_id:910000003, rating:5, votes:2310, comment_time:'2026-03-17 22:05:00', content:'法庭听证与主角独白都很有张力，历史厚重感很强。'},
  {user_md5:'e10adc3949ba59abbe56e057f20f883e', movie_id:90000002, comment_id:910000004, rating:5, votes:1222, comment_time:'2026-03-20 10:38:00', content:'世界观构建很完整，沙漠场景和音效都非常震撼。'},
  {user_md5:'25d55ad283aa400af464c76d713c07ad', movie_id:25845392, comment_id:910000005, rating:4, votes:740, comment_time:'2026-03-19 13:18:00', content:'战争场面震撼，群像塑造也有力度。'},
  {user_md5:'d8578edf8458ce06fbc5bb76a58c5ca4', movie_id:90000003, comment_id:910000006, rating:5, votes:1678, comment_time:'2026-03-20 14:30:00', content:'多元宇宙叙事节奏很快，动作场面和情感线都在线。'},
  {user_md5:'96e79218965eb72c92a549dd5a330112', movie_id:30474725, comment_id:910000007, rating:3, votes:451, comment_time:'2026-03-20 16:12:00', content:'悬疑推进不错，但个别桥段有点刻意。'},
  {user_md5:'6cb75f652a9b52798eb6cf2201057c73', movie_id:34780991, comment_id:910000008, rating:4, votes:590, comment_time:'2026-03-20 18:02:00', content:'世界观搭起来了，期待后两部。'},
  {user_md5:'c33367701511b4f6020ec61ded352059', movie_id:90000004, comment_id:910000009, rating:4, votes:503, comment_time:'2026-03-20 19:55:00', content:'飞行实拍很过瘾，节奏紧凑，视听体验非常强。'},
  {user_md5:'b59c67bf196a4758191e42f76670ceba', movie_id:34841067, comment_id:910000010, rating:4, votes:989, comment_time:'2026-03-20 21:11:00', content:'后半段的情绪很到位，影院里不少人都在擦眼泪。'}
] AS row
MATCH (u:User {user_md5: row.user_md5})
MATCH (m:Movie {movie_id: row.movie_id})
MERGE (u)-[r:COMMENTED {comment_id: row.comment_id}]->(m)
SET r.content = row.content,
    r.votes = row.votes,
    r.comment_time = row.comment_time,
    r.rating = row.rating,
    r.source_system = 'movie_reviews';
