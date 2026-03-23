const dbName = "movie_mongodb_db";
const movieDb = db.getSiblingDB(dbName);

function ensureCollection(name, options = {}) {
  const exists = movieDb.getCollectionNames().includes(name);
  if (!exists) {
    movieDb.createCollection(name, options);
  } else if (options.validator) {
    movieDb.runCommand({ collMod: name, validator: options.validator });
  }
}

ensureCollection("movie_reviews");
ensureCollection("movie_tags");
ensureCollection("rating_docs");
ensureCollection("recommendation_logs");
ensureCollection("user_behavior_docs");

ensureCollection("browse_history_docs", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["browse_id", "user_md5", "movie_id", "browse_time"],
      properties: {
        browse_id: { bsonType: ["long", "int", "string"] },
        user_md5: { bsonType: "string" },
        movie_id: { bsonType: ["long", "int", "string"] },
        browse_time: { bsonType: ["date", "string"] }
      }
    }
  }
});

ensureCollection("favorite_docs", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["favorite_id", "user_md5", "movie_id", "favorite_time", "status"],
      properties: {
        favorite_id: { bsonType: ["long", "int", "string"] },
        user_md5: { bsonType: "string" },
        movie_id: { bsonType: ["long", "int", "string"] },
        favorite_time: { bsonType: ["date", "string"] },
        status: { bsonType: "string" }
      }
    }
  }
});

function recreateIndex(collectionName, key, options) {
  const coll = movieDb.getCollection(collectionName);
  if (options && options.name) {
    try {
      coll.dropIndex(options.name);
    } catch (e) {
      // ignore when index does not exist
    }
  }
  coll.createIndex(key, options);
}

function dropAllNonIdIndexes(collectionName) {
  const coll = movieDb.getCollection(collectionName);
  coll.getIndexes().forEach((idx) => {
    if (idx.name !== "_id_") {
      try {
        coll.dropIndex(idx.name);
      } catch (e) {
        // ignore when index does not exist
      }
    }
  });
}

[
  "movie_reviews",
  "movie_tags",
  "rating_docs",
  "recommendation_logs",
  "user_behavior_docs",
  "browse_history_docs",
  "favorite_docs"
].forEach(dropAllNonIdIndexes);

recreateIndex("movie_reviews", { comment_id: 1 }, { unique: true, name: "uk_movie_reviews_comment_id" });
recreateIndex("movie_reviews", { movie_id: 1, comment_time: -1 }, { name: "idx_movie_reviews_movie_time" });
recreateIndex("movie_tags", { movie_id: 1 }, { name: "idx_movie_tags_movie_id" });
recreateIndex("rating_docs", { rating_id: 1 }, { unique: true, name: "uk_rating_docs_rating_id" });
recreateIndex("rating_docs", { user_md5: 1, movie_id: 1, rating_time: -1 }, { name: "idx_rating_docs_user_movie_time" });
recreateIndex("recommendation_logs", { user_md5: 1, recommend_time: -1 }, { name: "idx_recommendation_logs_user_time" });
recreateIndex("user_behavior_docs", { user_md5: 1, event_time: -1 }, { name: "idx_user_behavior_docs_user_time" });
recreateIndex("browse_history_docs", { browse_id: 1 }, { unique: true, name: "uk_browse_history_docs_browse_id" });
recreateIndex("browse_history_docs", { user_md5: 1, browse_time: -1 }, { name: "idx_browse_history_docs_user_time" });
recreateIndex("favorite_docs", { favorite_id: 1 }, { unique: true, name: "uk_favorite_docs_favorite_id" });
recreateIndex("favorite_docs", { user_md5: 1, status: 1, favorite_time: -1 }, { name: "idx_favorite_docs_user_status_time" });

[
  "movie_reviews",
  "movie_tags",
  "rating_docs",
  "recommendation_logs",
  "user_behavior_docs",
  "browse_history_docs",
  "favorite_docs"
].forEach((name) => movieDb.getCollection(name).deleteMany({}));

movieDb.movie_reviews.insertMany([
  {
    comment_id: NumberLong("910000001"),
    user_md5: "0ab7e3efacd56983f16503572d2b9915",
    movie_id: NumberLong("35267208"),
    content: "工业质感比前作更稳，前段铺垫稍慢，后段情绪拉满。",
    votes: 1280,
    comment_time: ISODate("2026-03-19T12:20:00Z"),
    rating: 5,
    review_status: "PUBLISHED",
    source: "app",
    language: "zh-CN",
    sentiment: { label: "positive", score: 0.93 }
  },
  {
    comment_id: NumberLong("910000002"),
    user_md5: "5f4dcc3b5aa765d61d8327deb882cf99",
    movie_id: NumberLong("36779979"),
    content: "拳击训练段落很扎实，后半段情绪爆发非常有感染力。",
    votes: 860,
    comment_time: ISODate("2026-03-18T09:14:00Z"),
    rating: 4,
    review_status: "PUBLISHED",
    source: "app",
    language: "zh-CN",
    sentiment: { label: "positive", score: 0.88 }
  },
  {
    comment_id: NumberLong("910000003"),
    user_md5: "9e107d9d372bb6826bd81d3542a419d6",
    movie_id: NumberLong("90000001"),
    content: "法庭听证与主角独白都很有张力，历史厚重感很强。",
    votes: 2310,
    comment_time: ISODate("2026-03-17T14:05:00Z"),
    rating: 5,
    review_status: "PUBLISHED",
    source: "web",
    language: "zh-CN",
    sentiment: { label: "positive", score: 0.97 }
  },
  {
    comment_id: NumberLong("910000004"),
    user_md5: "e10adc3949ba59abbe56e057f20f883e",
    movie_id: NumberLong("90000002"),
    content: "世界观构建很完整，沙漠场景和音效都非常震撼。",
    votes: 1222,
    comment_time: ISODate("2026-03-20T02:38:00Z"),
    rating: 5,
    review_status: "PUBLISHED",
    source: "app",
    language: "zh-CN",
    sentiment: { label: "positive", score: 0.91 }
  },
  {
    comment_id: NumberLong("910000005"),
    user_md5: "25d55ad283aa400af464c76d713c07ad",
    movie_id: NumberLong("25845392"),
    content: "战争场面震撼，群像塑造也有力度。",
    votes: 740,
    comment_time: ISODate("2026-03-19T05:18:00Z"),
    rating: 4,
    review_status: "PUBLISHED",
    source: "app",
    language: "zh-CN",
    sentiment: { label: "positive", score: 0.84 }
  },
  {
    comment_id: NumberLong("910000006"),
    user_md5: "d8578edf8458ce06fbc5bb76a58c5ca4",
    movie_id: NumberLong("90000003"),
    content: "多元宇宙叙事节奏很快，动作场面和情感线都在线。",
    votes: 1678,
    comment_time: ISODate("2026-03-20T06:30:00Z"),
    rating: 5,
    review_status: "PUBLISHED",
    source: "web",
    language: "zh-CN",
    sentiment: { label: "positive", score: 0.95 }
  },
  {
    comment_id: NumberLong("910000007"),
    user_md5: "96e79218965eb72c92a549dd5a330112",
    movie_id: NumberLong("30474725"),
    content: "悬疑推进不错，但个别桥段有点刻意。",
    votes: 451,
    comment_time: ISODate("2026-03-20T08:12:00Z"),
    rating: 3,
    review_status: "PUBLISHED",
    source: "app",
    language: "zh-CN",
    sentiment: { label: "neutral", score: 0.56 }
  },
  {
    comment_id: NumberLong("910000008"),
    user_md5: "6cb75f652a9b52798eb6cf2201057c73",
    movie_id: NumberLong("34780991"),
    content: "世界观搭起来了，期待后两部。",
    votes: 590,
    comment_time: ISODate("2026-03-20T10:02:00Z"),
    rating: 4,
    review_status: "PUBLISHED",
    source: "app",
    language: "zh-CN",
    sentiment: { label: "positive", score: 0.82 }
  },
  {
    comment_id: NumberLong("910000009"),
    user_md5: "c33367701511b4f6020ec61ded352059",
    movie_id: NumberLong("90000004"),
    content: "节奏快，笑点和推理平衡得不错。",
    votes: 503,
    comment_time: ISODate("2026-03-20T11:55:00Z"),
    rating: 4,
    review_status: "PUBLISHED",
    source: "mini_program",
    language: "zh-CN",
    sentiment: { label: "positive", score: 0.80 }
  },
  {
    comment_id: NumberLong("910000010"),
    user_md5: "b59c67bf196a4758191e42f76670ceba",
    movie_id: NumberLong("34841067"),
    content: "后半段的情绪很到位，影院里不少人都在擦眼泪。",
    votes: 989,
    comment_time: ISODate("2026-03-20T13:11:00Z"),
    rating: 4,
    review_status: "PUBLISHED",
    source: "app",
    language: "zh-CN",
    sentiment: { label: "positive", score: 0.90 }
  }
]);

movieDb.movie_tags.insertMany([
  {
    movie_id: NumberLong("36779979"),
    genres: ["剧情", "喜剧", "运动"],
    tags: [
      { name: "拳击", weight: 0.93, source: "ugc" },
      { name: "女性成长", weight: 0.88, source: "ugc" },
      { name: "春节档", weight: 0.77, source: "ops" }
    ],
    year: 2024,
    updated_time: ISODate("2026-03-20T09:00:00Z")
  },
  {
    movie_id: NumberLong("35267208"),
    genres: ["科幻", "冒险", "灾难"],
    tags: [
      { name: "太空电梯", weight: 0.91, source: "ugc" },
      { name: "数字生命", weight: 0.87, source: "ops" },
      { name: "春节档", weight: 0.69, source: "ops" }
    ],
    year: 2023,
    updated_time: ISODate("2026-03-20T09:00:00Z")
  },
  {
    movie_id: NumberLong("90000001"),
    genres: ["剧情", "传记", "历史"],
    tags: [
      { name: "核物理", weight: 0.86, source: "ugc" },
      { name: "奥斯卡", weight: 0.83, source: "ops" }
    ],
    year: 2023,
    updated_time: ISODate("2026-03-20T09:00:00Z")
  },
  {
    movie_id: NumberLong("90000002"),
    genres: ["科幻", "动作", "冒险"],
    tags: [
      { name: "厄拉科斯", weight: 0.87, source: "ugc" },
      { name: "IMAX", weight: 0.82, source: "ops" }
    ],
    year: 2024,
    updated_time: ISODate("2026-03-20T09:00:00Z")
  },
  {
    movie_id: NumberLong("25845392"),
    genres: ["历史", "战争"],
    tags: [
      { name: "战争", weight: 0.93, source: "ugc" },
      { name: "历史", weight: 0.76, source: "ugc" }
    ],
    year: 2021,
    updated_time: ISODate("2026-03-20T09:00:00Z")
  },
  {
    movie_id: NumberLong("90000003"),
    genres: ["动作", "科幻", "奇幻"],
    tags: [
      { name: "漫威", weight: 0.90, source: "ugc" },
      { name: "多元宇宙", weight: 0.88, source: "ops" }
    ],
    year: 2021,
    updated_time: ISODate("2026-03-20T09:00:00Z")
  },
  {
    movie_id: NumberLong("30474725"),
    genres: ["剧情", "喜剧", "悬疑"],
    tags: [
      { name: "反转", weight: 0.74, source: "ugc" },
      { name: "古装", weight: 0.68, source: "ugc" }
    ],
    year: 2023,
    updated_time: ISODate("2026-03-20T09:00:00Z")
  },
  {
    movie_id: NumberLong("34780991"),
    genres: ["动作", "奇幻", "战争"],
    tags: [
      { name: "神话史诗", weight: 0.88, source: "ops" },
      { name: "封神宇宙", weight: 0.81, source: "ugc" }
    ],
    year: 2023,
    updated_time: ISODate("2026-03-20T09:00:00Z")
  },
  {
    movie_id: NumberLong("90000004"),
    genres: ["动作", "剧情"],
    tags: [
      { name: "航空", weight: 0.86, source: "ugc" },
      { name: "续作", weight: 0.79, source: "ops" }
    ],
    year: 2022,
    updated_time: ISODate("2026-03-20T09:00:00Z")
  },
  {
    movie_id: NumberLong("34841067"),
    genres: ["剧情", "喜剧", "奇幻"],
    tags: [
      { name: "亲情", weight: 0.95, source: "ugc" },
      { name: "春节档", weight: 0.78, source: "ops" }
    ],
    year: 2021,
    updated_time: ISODate("2026-03-20T09:00:00Z")
  }
]);

movieDb.rating_docs.insertMany([
  { rating_id: NumberLong("1359352573"), user_md5: "0ab7e3efacd56983f16503572d2b9915", movie_id: NumberLong("35267208"), rating: 2, rating_time: ISODate("2025-11-05T11:42:07Z"), source: "douban_history" },
  { rating_id: NumberLong("920000001"), user_md5: "0ab7e3efacd56983f16503572d2b9915", movie_id: NumberLong("35267208"), rating: 5, rating_time: ISODate("2026-03-19T12:18:00Z"), source: "app" },
  { rating_id: NumberLong("920000002"), user_md5: "5f4dcc3b5aa765d61d8327deb882cf99", movie_id: NumberLong("36779979"), rating: 4, rating_time: ISODate("2026-03-18T09:10:00Z"), source: "app" },
  { rating_id: NumberLong("920000003"), user_md5: "9e107d9d372bb6826bd81d3542a419d6", movie_id: NumberLong("90000001"), rating: 5, rating_time: ISODate("2026-03-17T14:00:00Z"), source: "web" },
  { rating_id: NumberLong("920000004"), user_md5: "e10adc3949ba59abbe56e057f20f883e", movie_id: NumberLong("90000002"), rating: 5, rating_time: ISODate("2026-03-20T02:35:00Z"), source: "app" },
  { rating_id: NumberLong("920000005"), user_md5: "25d55ad283aa400af464c76d713c07ad", movie_id: NumberLong("25845392"), rating: 4, rating_time: ISODate("2026-03-19T05:15:00Z"), source: "app" },
  { rating_id: NumberLong("920000006"), user_md5: "d8578edf8458ce06fbc5bb76a58c5ca4", movie_id: NumberLong("90000003"), rating: 5, rating_time: ISODate("2026-03-20T06:28:00Z"), source: "web" },
  { rating_id: NumberLong("920000007"), user_md5: "96e79218965eb72c92a549dd5a330112", movie_id: NumberLong("30474725"), rating: 3, rating_time: ISODate("2026-03-20T08:10:00Z"), source: "app" },
  { rating_id: NumberLong("920000008"), user_md5: "6cb75f652a9b52798eb6cf2201057c73", movie_id: NumberLong("34780991"), rating: 4, rating_time: ISODate("2026-03-20T09:58:00Z"), source: "app" },
  { rating_id: NumberLong("920000009"), user_md5: "c33367701511b4f6020ec61ded352059", movie_id: NumberLong("90000004"), rating: 4, rating_time: ISODate("2026-03-20T11:53:00Z"), source: "mini_program" },
  { rating_id: NumberLong("920000010"), user_md5: "b59c67bf196a4758191e42f76670ceba", movie_id: NumberLong("34841067"), rating: 4, rating_time: ISODate("2026-03-20T13:06:00Z"), source: "app" }
]);

movieDb.recommendation_logs.insertMany([
  {
    rec_id: "rec_20260320_0001",
    user_md5: "0ab7e3efacd56983f16503572d2b9915",
    movie_id: NumberLong("35267208"),
    recommend_score: 0.973,
    recommend_time: ISODate("2026-03-20T09:05:00Z"),
    scene: "home_feed",
    model_version: "fm_v3.2.1",
    reason: [
      { type: "genre", value: "科幻" },
      { type: "recent_click", value: "36779979" }
    ],
    status: "DELIVERED",
    clicked: true,
    click_time: ISODate("2026-03-20T09:05:32Z")
  },
  {
    rec_id: "rec_20260320_0002",
    user_md5: "5f4dcc3b5aa765d61d8327deb882cf99",
    movie_id: NumberLong("25845392"),
    recommend_score: 0.882,
    recommend_time: ISODate("2026-03-20T10:02:00Z"),
    scene: "detail_page",
    model_version: "fm_v3.2.1",
    reason: [{ type: "topic", value: "历史战争" }],
    status: "DELIVERED",
    clicked: false
  },
  {
    rec_id: "rec_20260320_0003",
    user_md5: "9e107d9d372bb6826bd81d3542a419d6",
    movie_id: NumberLong("90000001"),
    recommend_score: 0.915,
    recommend_time: ISODate("2026-03-20T10:50:00Z"),
    scene: "classic_channel",
    model_version: "fm_v3.2.1",
    reason: [{ type: "long_term_pref", value: "剧情" }],
    status: "DELIVERED",
    clicked: true,
    click_time: ISODate("2026-03-20T10:51:13Z")
  },
  {
    rec_id: "rec_20260320_0004",
    user_md5: "e10adc3949ba59abbe56e057f20f883e",
    movie_id: NumberLong("90000002"),
    recommend_score: 0.901,
    recommend_time: ISODate("2026-03-20T11:08:00Z"),
    scene: "animation_tab",
    model_version: "fm_v3.2.1",
    reason: [{ type: "genre", value: "动画" }],
    status: "DELIVERED",
    clicked: true,
    click_time: ISODate("2026-03-20T11:08:20Z")
  },
  {
    rec_id: "rec_20260320_0005",
    user_md5: "25d55ad283aa400af464c76d713c07ad",
    movie_id: NumberLong("90000003"),
    recommend_score: 0.887,
    recommend_time: ISODate("2026-03-20T12:00:00Z"),
    scene: "home_feed",
    model_version: "fm_v3.2.1",
    reason: [{ type: "co_watch", value: "现实题材" }],
    status: "DELIVERED",
    clicked: false
  },
  {
    rec_id: "rec_20260320_0006",
    user_md5: "d8578edf8458ce06fbc5bb76a58c5ca4",
    movie_id: NumberLong("34841067"),
    recommend_score: 0.861,
    recommend_time: ISODate("2026-03-20T12:37:00Z"),
    scene: "home_feed",
    model_version: "fm_v3.2.1",
    reason: [{ type: "similar_user", value: "group_12" }],
    status: "DELIVERED",
    clicked: true,
    click_time: ISODate("2026-03-20T12:38:14Z")
  }
]);

movieDb.user_behavior_docs.insertMany([
  {
    behavior_id: "bhv_000001",
    user_md5: "0ab7e3efacd56983f16503572d2b9915",
    movie_id: NumberLong("35267208"),
    event_type: "view_detail",
    event_time: ISODate("2026-03-20T09:04:41Z"),
    payload: { from_page: "home_feed", stay_seconds: 95, city: "北京", device: "iPhone 13" }
  },
  {
    behavior_id: "bhv_000002",
    user_md5: "0ab7e3efacd56983f16503572d2b9915",
    movie_id: NumberLong("35267208"),
    event_type: "play_trailer",
    event_time: ISODate("2026-03-20T09:05:05Z"),
    payload: { trailer_id: "tr_35267208_01", watch_percent: 0.92 }
  },
  {
    behavior_id: "bhv_000003",
    user_md5: "5f4dcc3b5aa765d61d8327deb882cf99",
    movie_id: NumberLong("36779979"),
    event_type: "view_detail",
    event_time: ISODate("2026-03-20T10:01:14Z"),
    payload: { from_page: "search", keyword: "热辣滚烫" }
  },
  {
    behavior_id: "bhv_000004",
    user_md5: "9e107d9d372bb6826bd81d3542a419d6",
    movie_id: NumberLong("90000001"),
    event_type: "post_review",
    event_time: ISODate("2026-03-20T10:52:06Z"),
    payload: { comment_id: NumberLong("910000003"), text_length: 28 }
  },
  {
    behavior_id: "bhv_000005",
    user_md5: "e10adc3949ba59abbe56e057f20f883e",
    movie_id: NumberLong("90000002"),
    event_type: "share_movie",
    event_time: ISODate("2026-03-20T11:09:30Z"),
    payload: { channel: "wechat", target: "friend" }
  },
  {
    behavior_id: "bhv_000006",
    user_md5: "25d55ad283aa400af464c76d713c07ad",
    movie_id: NumberLong("90000003"),
    event_type: "view_detail",
    event_time: ISODate("2026-03-20T12:01:15Z"),
    payload: { from_page: "recommend", rec_id: "rec_20260320_0005" }
  },
  {
    behavior_id: "bhv_000007",
    user_md5: "d8578edf8458ce06fbc5bb76a58c5ca4",
    movie_id: NumberLong("34841067"),
    event_type: "favorite",
    event_time: ISODate("2026-03-20T12:39:21Z"),
    payload: { favorite_id: NumberLong("970000006") }
  },
  {
    behavior_id: "bhv_000008",
    user_md5: "96e79218965eb72c92a549dd5a330112",
    movie_id: NumberLong("30474725"),
    event_type: "rate_movie",
    event_time: ISODate("2026-03-20T13:11:10Z"),
    payload: { rating_id: NumberLong("920000007"), rating: 3 }
  },
  {
    behavior_id: "bhv_000009",
    user_md5: "6cb75f652a9b52798eb6cf2201057c73",
    movie_id: NumberLong("34780991"),
    event_type: "view_detail",
    event_time: ISODate("2026-03-20T13:35:40Z"),
    payload: { from_page: "home_feed", stay_seconds: 78 }
  },
  {
    behavior_id: "bhv_000010",
    user_md5: "c33367701511b4f6020ec61ded352059",
    movie_id: NumberLong("90000004"),
    event_type: "search",
    event_time: ISODate("2026-03-20T14:02:21Z"),
    payload: { keyword: "壮志凌云2", result_rank: 1 }
  },
  {
    behavior_id: "bhv_000011",
    user_md5: "b59c67bf196a4758191e42f76670ceba",
    movie_id: NumberLong("34841067"),
    event_type: "view_detail",
    event_time: ISODate("2026-03-20T14:26:03Z"),
    payload: { from_page: "topic", topic_name: "春节档口碑" }
  }
]);

movieDb.browse_history_docs.insertMany([
  { browse_id: NumberLong("960000001"), user_md5: "0ab7e3efacd56983f16503572d2b9915", movie_id: NumberLong("35267208"), browse_time: ISODate("2026-03-20T09:04:41Z"), source_page: "home_feed", stay_seconds: 95, watch_intent: "HIGH" },
  { browse_id: NumberLong("960000002"), user_md5: "5f4dcc3b5aa765d61d8327deb882cf99", movie_id: NumberLong("36779979"), browse_time: ISODate("2026-03-20T10:01:14Z"), source_page: "search", stay_seconds: 74, watch_intent: "MEDIUM" },
  { browse_id: NumberLong("960000003"), user_md5: "9e107d9d372bb6826bd81d3542a419d6", movie_id: NumberLong("90000001"), browse_time: ISODate("2026-03-20T10:50:35Z"), source_page: "classic_channel", stay_seconds: 120, watch_intent: "HIGH" },
  { browse_id: NumberLong("960000004"), user_md5: "e10adc3949ba59abbe56e057f20f883e", movie_id: NumberLong("90000002"), browse_time: ISODate("2026-03-20T11:07:42Z"), source_page: "sci_fi_tab", stay_seconds: 66, watch_intent: "MEDIUM" },
  { browse_id: NumberLong("960000005"), user_md5: "25d55ad283aa400af464c76d713c07ad", movie_id: NumberLong("90000003"), browse_time: ISODate("2026-03-20T12:01:15Z"), source_page: "recommend", stay_seconds: 81, watch_intent: "HIGH" },
  { browse_id: NumberLong("960000006"), user_md5: "d8578edf8458ce06fbc5bb76a58c5ca4", movie_id: NumberLong("34841067"), browse_time: ISODate("2026-03-20T12:38:40Z"), source_page: "home_feed", stay_seconds: 58, watch_intent: "MEDIUM" },
  { browse_id: NumberLong("960000007"), user_md5: "96e79218965eb72c92a549dd5a330112", movie_id: NumberLong("30474725"), browse_time: ISODate("2026-03-20T13:10:39Z"), source_page: "detail_recall", stay_seconds: 52, watch_intent: "LOW" },
  { browse_id: NumberLong("960000008"), user_md5: "6cb75f652a9b52798eb6cf2201057c73", movie_id: NumberLong("34780991"), browse_time: ISODate("2026-03-20T13:35:40Z"), source_page: "home_feed", stay_seconds: 78, watch_intent: "HIGH" },
  { browse_id: NumberLong("960000009"), user_md5: "c33367701511b4f6020ec61ded352059", movie_id: NumberLong("90000004"), browse_time: ISODate("2026-03-20T14:02:21Z"), source_page: "search_result", stay_seconds: 69, watch_intent: "MEDIUM" },
  { browse_id: NumberLong("960000010"), user_md5: "b59c67bf196a4758191e42f76670ceba", movie_id: NumberLong("34841067"), browse_time: ISODate("2026-03-20T14:26:03Z"), source_page: "topic", stay_seconds: 92, watch_intent: "HIGH" }
]);

movieDb.favorite_docs.insertMany([
  { favorite_id: NumberLong("970000001"), user_md5: "0ab7e3efacd56983f16503572d2b9915", movie_id: NumberLong("35267208"), favorite_time: ISODate("2026-03-20T09:06:00Z"), status: "ACTIVE", source: "detail_page" },
  { favorite_id: NumberLong("970000002"), user_md5: "5f4dcc3b5aa765d61d8327deb882cf99", movie_id: NumberLong("36779979"), favorite_time: ISODate("2026-03-20T10:04:12Z"), status: "ACTIVE", source: "detail_page" },
  { favorite_id: NumberLong("970000003"), user_md5: "9e107d9d372bb6826bd81d3542a419d6", movie_id: NumberLong("90000001"), favorite_time: ISODate("2026-03-20T10:53:44Z"), status: "ACTIVE", source: "classic_channel" },
  { favorite_id: NumberLong("970000004"), user_md5: "e10adc3949ba59abbe56e057f20f883e", movie_id: NumberLong("90000002"), favorite_time: ISODate("2026-03-20T11:10:11Z"), status: "ACTIVE", source: "sci_fi_tab" },
  { favorite_id: NumberLong("970000005"), user_md5: "25d55ad283aa400af464c76d713c07ad", movie_id: NumberLong("90000003"), favorite_time: ISODate("2026-03-20T12:02:30Z"), status: "ACTIVE", source: "recommend" },
  { favorite_id: NumberLong("970000006"), user_md5: "d8578edf8458ce06fbc5bb76a58c5ca4", movie_id: NumberLong("34841067"), favorite_time: ISODate("2026-03-20T12:39:21Z"), status: "ACTIVE", source: "detail_page" },
  { favorite_id: NumberLong("970000007"), user_md5: "96e79218965eb72c92a549dd5a330112", movie_id: NumberLong("30474725"), favorite_time: ISODate("2026-03-20T13:12:03Z"), status: "CANCELED", source: "detail_page", cancel_time: ISODate("2026-03-20T15:20:00Z") },
  { favorite_id: NumberLong("970000008"), user_md5: "6cb75f652a9b52798eb6cf2201057c73", movie_id: NumberLong("34780991"), favorite_time: ISODate("2026-03-20T13:37:12Z"), status: "ACTIVE", source: "home_feed" },
  { favorite_id: NumberLong("970000009"), user_md5: "c33367701511b4f6020ec61ded352059", movie_id: NumberLong("90000004"), favorite_time: ISODate("2026-03-20T14:04:39Z"), status: "ACTIVE", source: "search_result" },
  { favorite_id: NumberLong("970000010"), user_md5: "b59c67bf196a4758191e42f76670ceba", movie_id: NumberLong("34841067"), favorite_time: ISODate("2026-03-20T14:27:50Z"), status: "ACTIVE", source: "topic" }
]);

print("MongoDB sample data loaded into: " + dbName);
printjson({
  movie_reviews: movieDb.movie_reviews.countDocuments(),
  movie_tags: movieDb.movie_tags.countDocuments(),
  rating_docs: movieDb.rating_docs.countDocuments(),
  recommendation_logs: movieDb.recommendation_logs.countDocuments(),
  user_behavior_docs: movieDb.user_behavior_docs.countDocuments(),
  browse_history_docs: movieDb.browse_history_docs.countDocuments(),
  favorite_docs: movieDb.favorite_docs.countDocuments()
});


