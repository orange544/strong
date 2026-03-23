DROP SCHEMA IF EXISTS movie CASCADE;
CREATE SCHEMA movie;
SET search_path TO movie;

CREATE TABLE genre_dictionary (
  genre_code VARCHAR(32) PRIMARY KEY,
  genre_name_zh VARCHAR(64) NOT NULL,
  genre_name_en VARCHAR(64),
  genre_group VARCHAR(32),
  standard_status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
  created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE country_dictionary (
  country_code VARCHAR(16) PRIMARY KEY,
  country_name_zh VARCHAR(64) NOT NULL,
  country_name_en VARCHAR(64),
  region_group VARCHAR(32),
  standard_status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
  created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE language_dictionary (
  language_code VARCHAR(16) PRIMARY KEY,
  language_name_zh VARCHAR(64) NOT NULL,
  language_name_en VARCHAR(64),
  iso_639_1 VARCHAR(8),
  standard_status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
  created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE actor_master (
  actor_id BIGINT PRIMARY KEY,
  standard_name_zh VARCHAR(255) NOT NULL,
  standard_name_en VARCHAR(255),
  sex VARCHAR(16),
  birth_date DATE,
  birth_place VARCHAR(255),
  profession VARCHAR(255),
  source_system VARCHAR(32) NOT NULL DEFAULT 'douban_dataset',
  created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE director_master (
  director_id BIGINT PRIMARY KEY,
  standard_name_zh VARCHAR(255) NOT NULL,
  standard_name_en VARCHAR(255),
  sex VARCHAR(16),
  birth_date DATE,
  birth_place VARCHAR(255),
  profession VARCHAR(255),
  source_system VARCHAR(32) NOT NULL DEFAULT 'douban_dataset',
  created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE movie_master (
  movie_id BIGINT PRIMARY KEY,
  standard_movie_code VARCHAR(32) NOT NULL UNIQUE,
  standard_name_zh VARCHAR(255) NOT NULL,
  standard_name_en VARCHAR(255),
  original_name VARCHAR(255),
  release_date DATE,
  release_year INT,
  duration_minutes INT,
  imdb_id VARCHAR(32),
  douban_score NUMERIC(3,1),
  douban_votes INT,
  primary_genre_code VARCHAR(32) REFERENCES genre_dictionary(genre_code),
  primary_country_code VARCHAR(16) REFERENCES country_dictionary(country_code),
  primary_language_code VARCHAR(16) REFERENCES language_dictionary(language_code),
  storyline TEXT,
  source_system VARCHAR(32) NOT NULL DEFAULT 'douban_dataset',
  created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE movie_standard_id (
  source_system VARCHAR(32) NOT NULL,
  source_object VARCHAR(32) NOT NULL,
  source_id VARCHAR(128) NOT NULL,
  movie_id BIGINT NOT NULL REFERENCES movie_master(movie_id),
  standard_movie_code VARCHAR(32) NOT NULL REFERENCES movie_master(standard_movie_code),
  source_name VARCHAR(64),
  is_primary BOOLEAN NOT NULL DEFAULT FALSE,
  created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (source_system, source_object, source_id)
);

CREATE TABLE movie_alias_mapping (
  alias_id BIGSERIAL PRIMARY KEY,
  movie_id BIGINT NOT NULL REFERENCES movie_master(movie_id),
  alias_name VARCHAR(255) NOT NULL,
  alias_language_code VARCHAR(16) REFERENCES language_dictionary(language_code),
  alias_type VARCHAR(32) NOT NULL,
  source_system VARCHAR(32) NOT NULL DEFAULT 'douban_dataset',
  created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (movie_id, alias_name)
);

CREATE TABLE movie_release_version (
  version_id BIGSERIAL PRIMARY KEY,
  movie_id BIGINT NOT NULL REFERENCES movie_master(movie_id),
  version_name VARCHAR(128) NOT NULL,
  region_code VARCHAR(16) REFERENCES country_dictionary(country_code),
  language_code VARCHAR(16) REFERENCES language_dictionary(language_code),
  release_date DATE,
  duration_minutes INT,
  is_re_release BOOLEAN NOT NULL DEFAULT FALSE,
  source_system VARCHAR(32) NOT NULL DEFAULT 'ticketing_system',
  created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (movie_id, version_name, region_code, release_date)
);

CREATE TABLE movie_actor_mapping (
  movie_id BIGINT NOT NULL REFERENCES movie_master(movie_id),
  actor_id BIGINT NOT NULL REFERENCES actor_master(actor_id),
  cast_order INT,
  role_name VARCHAR(255),
  is_lead BOOLEAN NOT NULL DEFAULT FALSE,
  PRIMARY KEY (movie_id, actor_id)
);

CREATE TABLE movie_director_mapping (
  movie_id BIGINT NOT NULL REFERENCES movie_master(movie_id),
  director_id BIGINT NOT NULL REFERENCES director_master(director_id),
  director_order INT,
  PRIMARY KEY (movie_id, director_id)
);

CREATE TABLE movie_genre_mapping (
  movie_id BIGINT NOT NULL REFERENCES movie_master(movie_id),
  genre_code VARCHAR(32) NOT NULL REFERENCES genre_dictionary(genre_code),
  is_primary BOOLEAN NOT NULL DEFAULT FALSE,
  PRIMARY KEY (movie_id, genre_code)
);

CREATE TABLE movie_country_mapping (
  movie_id BIGINT NOT NULL REFERENCES movie_master(movie_id),
  country_code VARCHAR(16) NOT NULL REFERENCES country_dictionary(country_code),
  is_primary BOOLEAN NOT NULL DEFAULT FALSE,
  PRIMARY KEY (movie_id, country_code)
);

CREATE TABLE movie_language_mapping (
  movie_id BIGINT NOT NULL REFERENCES movie_master(movie_id),
  language_code VARCHAR(16) NOT NULL REFERENCES language_dictionary(language_code),
  is_primary BOOLEAN NOT NULL DEFAULT FALSE,
  PRIMARY KEY (movie_id, language_code)
);

CREATE TABLE field_semantic_dictionary (
  semantic_id BIGSERIAL PRIMARY KEY,
  semantic_name VARCHAR(64) NOT NULL,
  semantic_description VARCHAR(255) NOT NULL,
  canonical_type VARCHAR(32) NOT NULL,
  canonical_format VARCHAR(64),
  source_system VARCHAR(32) NOT NULL,
  source_object VARCHAR(64) NOT NULL,
  source_field VARCHAR(64) NOT NULL,
  example_value VARCHAR(255),
  is_primary BOOLEAN NOT NULL DEFAULT FALSE,
  created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (semantic_name, source_system, source_object, source_field)
);

CREATE INDEX idx_movie_master_release_date ON movie_master(release_date);
CREATE INDEX idx_movie_alias_mapping_movie_id ON movie_alias_mapping(movie_id);
CREATE INDEX idx_movie_release_version_movie_id ON movie_release_version(movie_id);
CREATE INDEX idx_movie_standard_id_movie_id ON movie_standard_id(movie_id);
CREATE INDEX idx_field_semantic_dictionary_name ON field_semantic_dictionary(semantic_name);
