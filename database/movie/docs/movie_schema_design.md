# Movie Schema Design

## Design Basis
- Business roles are aligned with `实验环境构建.md` in the movie domain.
- Field roots are aligned with real dataset headers under `datasets/豆瓣电影数据集`:
  - `movies.csv`: `MOVIE_ID, NAME, IMDB_ID, DOUBAN_SCORE, DOUBAN_VOTES, RELEASE_DATE, ...`
  - `person.csv`: `PERSON_ID, NAME, NAME_EN, NAME_ZH, ...`
  - `users.csv`: `USER_MD5, USER_NICKNAME`
  - `ratings.csv`: `RATING_ID, USER_MD5, MOVIE_ID, RATING, RATING_TIME`
  - `comments.csv`: `COMMENT_ID, USER_MD5, MOVIE_ID, CONTENT, VOTES, COMMENT_TIME, RATING`

## Naming Principles
- Keep core identifiers stable across systems: `movie_id`, `person_id`, `user_md5`, `rating_id`, `comment_id`.
- Keep source-specific fields when they carry semantics: `douban_score`, `douban_votes`, `imdb_id`.
- Allow controlled heterogeneity by system:
  - Oracle uses `UPPER_SNAKE_CASE`.
  - Neo4j uses `PascalCase` labels + `UPPER_SNAKE_CASE` relationship types.
  - Redis uses namespaced keys.
  - HBase uses column families and rowkey patterns.

## MySQL
- Role: online ticketing transaction system.
- Objects:
  - `movie_user`: user account and identity bridge (`user_id`, `user_md5`).
  - `movie_basic`: movie master profile aligned with source fields.
  - `cinema`, `hall`, `seat`: theater topology and seat inventory.
  - `schedule`: showtime and sellable schedule.
  - `ticket_order`: order transaction header.
  - `payment_record`: payment lifecycle record.
- Naming reason: transaction fields follow business (`order_*`, `payment_*`), movie fields follow real dataset roots.

## PostgreSQL
- Role: semantic integration and analytical serving layer.
- Objects:
  - `dim_movie`, `dim_person`, `dim_user`: conformed dimensions.
  - `map_movie_person`: actor/director relation bridge.
  - `fact_rating`, `fact_comment`, `fact_ticket_order`: facts across behavior and transaction.
  - `id_mapping`: cross-system identity mapping.
- Naming reason: `dim_/fact_/map_` for warehouse readability, while column roots stay source-compatible.

## Oracle
- Role: copyright, contract, release authorization, settlement core.
- Objects:
  - `PUBLISHER_COMPANY`, `PRODUCTION_COMPANY`: enterprise organization master.
  - `MOVIE_COPYRIGHT`: copyright ownership and status.
  - `LICENSE_CONTRACT`: licensing contract lifecycle.
  - `RELEASE_REGION_AUTH`: region authorization and release control.
  - `SCHEDULE_APPROVAL`: release approval workflow.
  - `BOXOFFICE_SETTLEMENT_RULE`: settlement policy.
  - `PARTNER_SETTLEMENT_RECORD`: financial settlement records.
- Naming reason: preserve enterprise style (`UPPER_SNAKE_CASE`) and explicit status/time fields.

## SQLite
- Role: local offline sample and lightweight acquisition store.
- Objects:
  - `local_movie`, `local_person`, `local_user`: local dimensions.
  - `local_movie_person`: local relation bridge.
  - `offline_rating`, `browse_sample`: local behavior facts.
  - `local_schedule`: lightweight schedule cache.
  - `movie_meta_cache`: key-value metadata cache.
- Naming reason: keep source field roots for portability; keep local prefixes for offline context.

## MongoDB
- Role: semi-structured reviews, tags, recommendation, behavior logs.
- Objects:
  - `movie_reviews`: comment-centric documents.
  - `movie_tags`: tag/genre snapshots.
  - `rating_docs`: rating events.
  - `recommendation_logs`: recommendation outputs.
  - `user_behavior_docs`: flexible behavior events.
- Naming reason: collection names reflect business domains; validators enforce stable top-level field names.

## Neo4j
- Role: movie knowledge graph and semantic relation hub.
- Objects:
  - Node labels: `Movie`, `Person`, `User`, `Genre`, `Company`, `Country`, `Award`.
  - Relationship types: `ACTED_IN`, `DIRECTED_BY`, `BELONGS_TO`, `PRODUCED_BY`, `RELEASED_IN`, `WON_AWARD`, `RATED`, `COMMENTED`.
  - Constraints/indexes on core identifier/name fields.
- Naming reason: graph-native naming for labels and relations, with source-compatible property names.

## Redis
- Role: high-frequency cache and real-time state.
- Objects (key structures):
  - `movie:basic:{movie_id}`
  - `movie:hot:rank`
  - `schedule:seat:status:{schedule_id}`
  - `order:pending:{order_id}`
  - `payment:status:{order_id}`
  - `user:session:{user_md5}`
  - `recommend:temp:{user_md5}`
  - `query:result:{query_key}`
- Naming reason: namespace keys by domain/object/id to prevent collisions and keep clear ownership.

## ClickHouse
- Role: analytical warehouse for box office and behavior aggregates.
- Objects:
  - `boxoffice_fact`
  - `schedule_attendance_fact`
  - `city_boxoffice_stat`
  - `movie_rating_distribution`
  - `movie_popularity_trend`
  - `recommend_exposure_click_fact`
  - `search_heat_stat`
- Naming reason: analytical suffixes (`_fact`, `_stat`, `_trend`, `_distribution`) and metric fields (`*_count`, `*_rate`, `*_amount`).

## TiDB
- Role: distributed ticketing transaction system across regions.
- Objects:
  - `distributed_user`
  - `ticket_order_main`
  - `ticket_order_item`
  - `seat_inventory`
  - `cross_region_schedule`
  - `payment_flow`
  - `refund_record`
  - `promotion_usage`
- Naming reason: transaction objects mirror real ticketing pipeline, plus distributed-region fields (`region_code`).

## Cassandra
- Role: high-throughput append-only behavior timeline storage.
- Objects:
  - `movie_browse_event`
  - `movie_click_event`
  - `movie_search_event`
  - `favorite_event`
  - `recommend_exposure_event`
  - `recommend_click_event`
  - `rating_event`
  - `user_access_timeline`
- Naming reason: event tables use consistent time-series keys (`event_date`, `event_time`) and source-stable IDs.

## HBase
- Role: massive sparse metadata, archive, and index-mid storage.
- Objects:
  - `movie_meta_wide`
  - `movie_version_archive`
  - `person_ext_profile`
  - `movie_id_mapping`
  - `behavior_archive`
  - `crawler_movie_raw`
  - `search_index_mid`
  - `movie_tag_wide`
- Naming reason: table names represent storage intent (wide/archive/mapping/index), and column-family names map to access patterns.

## Delivery Notes
- All generated files are structure-only.
- No sample rows, `INSERT`, `MERGE`, or `put` data statements are included.
