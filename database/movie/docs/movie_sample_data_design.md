# Movie Sample Data Design

## Scope
This document currently records Neo4j, Redis, and ClickHouse sample data design and loading scope.

## Neo4j Data Files
- `neo4j/movie_neo4j_sample_data.cypher`
  - Purpose: load sample graph data for movie knowledge relations.
  - Content type: nodes + relationships only.
  - Shared keys: `movie_id`, `person_id`, `user_md5`, `rating_id`, `comment_id`.

## Redis Data Files
- `redis/movie_redis_sample_data.txt`
  - Purpose: load Redis movie cache sample data for shared query and cross-store linkage.
  - Content type: key initialization + sample values only (no new object types).
  - Shared keys: `movie_id`, `user_md5`, movie keyword cache keys.

## ClickHouse Data Files
- `clickhouse/movie_clickhouse.sql`
  - Purpose: initialize ClickHouse analytic schema and load movie analytic sample data.
  - Content type: fact/stat/trend/ranking analytic tables + sample rows.
  - Shared keys: `movie_id`, `user_md5`, `stat_date`, ranking `hot_score`.

## Neo4j: Data Source References
- Primary reference dataset: `datasets/豆瓣电影数据集`
  - `movies.csv` for movie IDs, movie titles, aliases, IMDb IDs, genres, regions, release dates, score style.
  - `person.csv` for person naming style (Chinese names with optional English names) and person ID style.
- Existing local sample references:
  - `mysql/movie_mysql_sample_data.sql` for aligned `movie_id` and `user_md5` values.
  - `postgresql/movie_postgresql_sample_data.sql` for standardized movie/person identity mappings.
  - `mongodb/movie_mongodb_sample_data.js` for review/rating behavioral value style.

## Neo4j: Field Value Styles from Real Data
- Movie identity and attributes follow dataset naming and value style:
  - `movie_id`, `name`, `name_en`, `alias`, `imdb_id`, `douban_score`, `douban_votes`, `release_date`, `year`.
- Person identity uses dataset style IDs and names:
  - `person_id`, `name`, `name_en`.
- User identity uses hash style from real dataset and existing MySQL/MongoDB samples:
  - `user_md5`, `user_nickname`.
- Relationship event fields reuse realistic behavior/log style:
  - `rating_id`, `rating`, `rating_time` on `RATED`.
  - `comment_id`, `content`, `votes`, `comment_time`, `rating` on `COMMENTED`.

## Neo4j: Supplementary Generated Data
The following are generated extensions for graph completeness (not direct one-to-one copy from raw CSV):
- `Company` nodes and `PRODUCED_BY` relations.
- `Award` nodes and `WON_AWARD` relations.
- Relationship metadata such as `cast_order`, `director_order`, `company_role`, `source_system`.
These additions are controlled and business-oriented to support downstream graph queries and semantic experiments.

## Neo4j: Cross-Database Shared Relations
Neo4j can be naturally linked to other databases through existing semantics:
- Movie shared key: `Movie.movie_id` <-> MySQL `movie_basic.movie_id` <-> PostgreSQL `movie_master.movie_id` <-> MongoDB `movie_id` fields.
- Person shared key: `Person.person_id` <-> PostgreSQL `actor_master.actor_id` / `director_master.director_id`.
- User shared key: `User.user_md5` <-> MySQL `movie_user.user_md5` <-> PostgreSQL `dim_user.user_md5` <-> MongoDB `user_md5` fields.
- Review/rating linkage:
  - Neo4j `COMMENTED.comment_id` <-> MongoDB `movie_reviews.comment_id`.
  - Neo4j `RATED.rating_id` <-> MongoDB `rating_docs.rating_id`.

## Neo4j: Best Candidates for Field Extraction and Semantic Unification
Most useful node types:
- `Movie`, `Person` (`Actor`/`Director` labels), `User`, `Genre`, `Country`, `Company`, `Award`.
Most useful relationship types:
- `ACTED_IN`, `DIRECTED_BY`, `BELONGS_TO`, `RELEASED_IN`, `PRODUCED_BY`, `WON_AWARD`, `RATED`, `COMMENTED`.
Why these are useful:
- They directly expose entity attributes + relation context.
- They preserve graph-native semantics while keeping compatible identity keys for cross-store joins.
- They support extraction of canonical names, multilingual naming, behavioral traces, and domain relationships.

## Redis: Data Source References
- Primary reference dataset: `datasets/豆瓣电影数据集`
  - `movies.csv` for movie identity and movie-side value style:
    - `MOVIE_ID`, `NAME`, `ALIAS`, `IMDB_ID`, `DOUBAN_SCORE`, `DOUBAN_VOTES`, `GENRES`, `LANGUAGES`, `REGIONS`, `RELEASE_DATE`.
  - `comments.csv` for comment aggregation style:
    - `MOVIE_ID`, `RATING`, `COMMENT_TIME`, `CONTENT` (used to derive summary keywords and timing style).
  - `ratings.csv` for user behavior identity style:
    - `USER_MD5`, `MOVIE_ID`, `RATING_TIME`.
- Existing local sample references:
  - `mysql/movie_mysql_sample_data.sql` for shared `movie_id` and `user_md5` values.
  - `mongodb/movie_mongodb_sample_data.js` for review text style and summary keyword style.
  - ClickHouse trend semantics are reflected in cache source markers (`source=clickhouse.movie_popularity_trend`).

## Redis: Field Value Styles from Real Data
- Movie identity and score style follows dataset convention:
  - `movie_id`, `name`, `imdb_id`, `douban_score`, `douban_votes`.
- Movie detail JSON keeps content-platform style with mixed normalized and cache fields:
  - `genres`, `languages`, `regions`, `release_date`, `year`, `updated_time`.
- User session hash follows behavior-system style and existing local user identifiers:
  - `user_md5`, `user_nickname`, `city`, `device`, `app_version`, `last_access_time`, `session_status`.
- Ranking and recommendation keep online serving style:
  - hot rank via `ZSET` score.
  - recommendation via per-user `LIST` of JSON payloads (`recommend_score`, `scene`, `rec_time`).

## Redis: Supplementary Generated Data
The following are generated to complete cache scenarios while keeping natural business semantics:
- Hot score values and rank ordering snapshots for `movie:hot:rank`.
- Query cache keyword samples (`liulangdiqiu`, `kehuan`, `spring_festival`, `war`, `classic`).
- Session recency timestamps and TTL policies.
- Recommendation payloads (`scene`, `recommend_score`) for online recommendation cache.
These are controlled additions and keep the same entity IDs used by upstream systems.

## Redis: Cross-Database Shared Relations
Redis keys can be mapped naturally to existing movie entities and analytics:
- Movie linkage:
  - `hot_movies:{movie_id}`, `movie:detail:{movie_id}`, `movie:comment:summary:{movie_id}` -> MySQL `movie_basic.movie_id` -> PostgreSQL `movie_master.movie_id` -> Neo4j `Movie.movie_id` -> MongoDB `movie_id`.
- User linkage:
  - `user:session:{user_md5}`, `rec:user:{user_md5}` -> MySQL `movie_user.user_md5` -> MongoDB user behavior docs -> Neo4j `User.user_md5`.
- Hotness/analysis linkage:
  - `movie:hot:rank` score and `hot_movies:{movie_id}.hot_score` correspond to ClickHouse popularity aggregates used for serving cache refresh.

## Redis: Best Candidates for Field Extraction and Semantic Unification
Most useful key patterns:
- `hot_movies:{movie_id}` (hash)
- `movie:detail:{movie_id}` (string JSON)
- `movie:hot:rank` (zset)
- `query:cache:movie:keyword:{keyword}` (string JSON)
- `user:session:{user_md5}` (hash)
- `rec:user:{user_md5}` (list JSON)
- `movie:comment:summary:{movie_id}` (hash)
Most useful fields for extraction:
- Identity fields: `movie_id`, `user_md5`, `imdb_id`
- Metric fields: `douban_score`, `douban_votes`, `hot_score`, `avg_rating`, `comment_count`
- Time fields: `stat_date`, `updated_time`, `query_time`, `rec_time`, `last_access_time`
- Semantic text fields: `name`, `genres`, `top_keywords`, `scene`
Why these are useful:
- They preserve online cache heterogeneity (hash/string/list/zset) while still exposing stable shared identifiers.
- They provide mixed structural and textual values for field extraction and semantic alignment tasks.
- They are directly joinable with graph, OLTP, document, and OLAP entities through existing IDs.

## ClickHouse: Data Source References
- Primary reference dataset: `datasets/豆瓣电影数据集`
  - `movies.csv`: movie identity and metadata style (`MOVIE_ID`, `NAME`, `ALIAS`, `IMDB_ID`, `DOUBAN_SCORE`, `DOUBAN_VOTES`, `GENRES`, `REGIONS`, `RELEASE_DATE`).
  - `ratings.csv`: scoring and time style (`USER_MD5`, `MOVIE_ID`, `RATING`, `RATING_TIME`) used to shape rating distribution and trend metrics.
  - `comments.csv`: comment timestamp and content activity style (`COMMENT_TIME`, `RATING`, `CONTENT`) used to shape discuss/comment/search trend intensity.
- Existing local sample references:
  - `mysql/movie_mysql_sample_data.sql`: schedule/cinema/order/payment style for box office, attendance, and city-level aggregates.
  - `redis/movie_redis_sample_data.txt`: hot score and ranking synchronization target (`hot_movies:*`, `movie:hot:rank`).
  - `neo4j/movie_neo4j_sample_data.cypher`: movie entity identity alignment via `movie_id`.

## ClickHouse: Field Value Styles from Real Data
- Identity fields directly reuse real data style:
  - `movie_id` (numeric movie identity), `user_md5` (hash user identity), `stat_date` (daily partition-style date).
- Metric fields follow real platform analytics conventions:
  - Box office: `ticket_order_count`, `payment_amount`.
  - Attendance: `seat_count`, `sold_seat_count`, `attendance_rate`.
  - Rating distribution: `rating`, `rating_count`.
  - Heat/recommend/search: `search_count`, `comment_count`, `exposure_count`, `click_count`, `conversion_rate`, `hot_score`.
- Text fields follow existing business language style:
  - `keyword` uses mixed Chinese business search terms (`流浪地球`, `科幻`, `春节档`, `战争`, `经典`).
  - `rank_type` uses analytic channel labels (`daily_hot`, `weekly_hot`).

## ClickHouse: Supplementary Generated Data
The following are generated extensions to complete analytic scenarios:
- Multi-day trend metrics (`2026-03-18` to `2026-03-23`) for popularity, box office, search, and recommendation exposure/click.
- City-level rollups (`CN-BJ`, `CN-SH`, `CN-CD`) derived from cinema-region style in local ticketing schema.
- Hot ranking snapshot object (`movie_hot_ranking_snapshot`) for point-in-time leaderboard analysis.
- Conversion metrics (`conversion_rate`) and attendance ratio (`attendance_rate`) computed from fact-style counts.
These are controlled supplements and remain aligned with existing movie IDs and user IDs.

## ClickHouse: Cross-Database Shared Relations
ClickHouse analytic objects are joinable with other stores through stable semantics:
- Movie linkage:
  - ClickHouse `movie_id` -> MySQL `movie_basic.movie_id` -> PostgreSQL `movie_master.movie_id` -> Neo4j `Movie.movie_id` -> MongoDB `movie_id`.
- User linkage:
  - ClickHouse `recommend_exposure_click_fact.user_md5` -> MySQL `movie_user.user_md5` -> MongoDB behavior docs -> Neo4j `User.user_md5`.
- Cache linkage:
  - ClickHouse `movie_popularity_trend.hot_score` and ranking snapshot -> Redis `hot_movies:{movie_id}.hot_score` and `movie:hot:rank`.
  - ClickHouse `search_heat_stat.keyword` + `movie_id` -> Redis query cache keys and result movie lists.

## ClickHouse: Best Candidates for Field Extraction and Semantic Unification
Most useful analytic tables:
- `boxoffice_fact`
- `schedule_attendance_fact`
- `city_boxoffice_stat`
- `movie_rating_distribution`
- `movie_popularity_trend`
- `recommend_exposure_click_fact`
- `search_heat_stat`
- `movie_hot_ranking_snapshot`
Most useful fields for extraction:
- Identity: `movie_id`, `user_md5`, `schedule_id`, `cinema_id`, `hall_id`
- Time: `stat_date`, `snapshot_time`
- Metrics: `payment_amount`, `ticket_order_count`, `attendance_rate`, `rating_count`, `search_count`, `click_count`, `conversion_rate`, `hot_score`
- Semantic/business text: `keyword`, `rank_type`, `source`, `region_code`
Why these are useful:
- They retain OLAP-style statistical semantics while preserving cross-store IDs.
- They expose both raw facts and derived metrics needed for field extraction and value-profile analysis.
- They provide a natural bridge from business events (MySQL/MongoDB) to serving cache (Redis) and graph entities (Neo4j).
