const dbName = "movie_mongodb_db";
const movieDb = db.getSiblingDB(dbName);

function ensureCollection(name, validator) {
  const exists = movieDb.getCollectionNames().includes(name);
  if (!exists) {
    movieDb.createCollection(name, { validator });
  } else {
    movieDb.runCommand({ collMod: name, validator });
  }
}

ensureCollection("movie_reviews", {
  $jsonSchema: {
    bsonType: "object",
    required: ["comment_id", "user_md5", "movie_id"],
    properties: {
      comment_id: { bsonType: ["long", "int", "string"] },
      user_md5: { bsonType: "string" },
      movie_id: { bsonType: ["long", "int", "string"] },
      content: { bsonType: "string" },
      votes: { bsonType: ["int", "long"] },
      comment_time: { bsonType: ["date", "string"] },
      rating: { bsonType: ["int", "long", "double"] }
    }
  }
});

ensureCollection("movie_tags", {
  $jsonSchema: {
    bsonType: "object",
    required: ["movie_id"],
    properties: {
      movie_id: { bsonType: ["long", "int", "string"] },
      genres: { bsonType: ["string", "array"] },
      tags: { bsonType: ["string", "array"] },
      year: { bsonType: ["int", "long", "string"] },
      updated_time: { bsonType: ["date", "string"] }
    }
  }
});

ensureCollection("rating_docs", {
  $jsonSchema: {
    bsonType: "object",
    required: ["rating_id", "user_md5", "movie_id", "rating"],
    properties: {
      rating_id: { bsonType: ["long", "int", "string"] },
      user_md5: { bsonType: "string" },
      movie_id: { bsonType: ["long", "int", "string"] },
      rating: { bsonType: ["int", "long", "double"] },
      rating_time: { bsonType: ["date", "string"] }
    }
  }
});

ensureCollection("recommendation_logs", {
  $jsonSchema: {
    bsonType: "object",
    required: ["user_md5", "movie_id", "recommend_score", "recommend_time"],
    properties: {
      user_md5: { bsonType: "string" },
      movie_id: { bsonType: ["long", "int", "string"] },
      recommend_score: { bsonType: ["double", "int", "long"] },
      recommend_time: { bsonType: ["date", "string"] },
      scene: { bsonType: "string" },
      model_version: { bsonType: "string" }
    }
  }
});

ensureCollection("user_behavior_docs", {
  $jsonSchema: {
    bsonType: "object",
    required: ["user_md5", "event_type", "event_time"],
    properties: {
      user_md5: { bsonType: "string" },
      movie_id: { bsonType: ["long", "int", "string"] },
      event_type: { bsonType: "string" },
      event_time: { bsonType: ["date", "string"] },
      payload: { bsonType: ["object", "string"] }
    }
  }
});

movieDb.movie_reviews.createIndex({ comment_id: 1 }, { unique: true, name: "uk_movie_reviews_comment_id" });
movieDb.movie_reviews.createIndex({ movie_id: 1, comment_time: -1 }, { name: "idx_movie_reviews_movie_time" });
movieDb.rating_docs.createIndex({ rating_id: 1 }, { unique: true, name: "uk_rating_docs_rating_id" });
movieDb.rating_docs.createIndex({ user_md5: 1, movie_id: 1, rating_time: -1 }, { name: "idx_rating_docs_user_movie_time" });
movieDb.movie_tags.createIndex({ movie_id: 1 }, { name: "idx_movie_tags_movie_id" });
movieDb.recommendation_logs.createIndex({ user_md5: 1, recommend_time: -1 }, { name: "idx_recommendation_logs_user_time" });
movieDb.user_behavior_docs.createIndex({ user_md5: 1, event_time: -1 }, { name: "idx_user_behavior_docs_user_time" });
