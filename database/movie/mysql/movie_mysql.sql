CREATE DATABASE IF NOT EXISTS movie_mysql_db CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE movie_mysql_db;

CREATE TABLE IF NOT EXISTS movie_user (
  user_id BIGINT PRIMARY KEY AUTO_INCREMENT,
  user_md5 CHAR(32) NOT NULL,
  user_nickname VARCHAR(128),
  phone VARCHAR(20),
  email VARCHAR(128),
  register_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  user_status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
  UNIQUE KEY uk_movie_user_md5 (user_md5),
  UNIQUE KEY uk_movie_user_phone (phone)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS movie_basic (
  movie_id BIGINT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  alias VARCHAR(500),
  actors TEXT,
  cover VARCHAR(500),
  directors TEXT,
  douban_score DECIMAL(3,1),
  douban_votes INT,
  genres VARCHAR(255),
  imdb_id VARCHAR(32),
  languages VARCHAR(255),
  mins INT,
  official_site VARCHAR(500),
  regions VARCHAR(255),
  release_date DATE,
  slug VARCHAR(128),
  storyline TEXT,
  tags TEXT,
  year INT,
  actor_ids TEXT,
  director_ids TEXT,
  created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_movie_basic_imdb_id (imdb_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS cinema (
  cinema_id BIGINT PRIMARY KEY AUTO_INCREMENT,
  cinema_name VARCHAR(255) NOT NULL,
  region_code VARCHAR(32) NOT NULL,
  address VARCHAR(500),
  cinema_status VARCHAR(16) NOT NULL DEFAULT 'OPEN',
  created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS hall (
  hall_id BIGINT PRIMARY KEY AUTO_INCREMENT,
  cinema_id BIGINT NOT NULL,
  hall_name VARCHAR(128) NOT NULL,
  seat_count INT NOT NULL,
  hall_status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
  created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_hall_cinema FOREIGN KEY (cinema_id) REFERENCES cinema(cinema_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS seat (
  seat_id BIGINT PRIMARY KEY AUTO_INCREMENT,
  hall_id BIGINT NOT NULL,
  row_no INT NOT NULL,
  col_no INT NOT NULL,
  seat_type VARCHAR(32),
  seat_status VARCHAR(16) NOT NULL DEFAULT 'AVAILABLE',
  CONSTRAINT fk_seat_hall FOREIGN KEY (hall_id) REFERENCES hall(hall_id),
  UNIQUE KEY uk_seat_position (hall_id, row_no, col_no)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS schedule (
  schedule_id BIGINT PRIMARY KEY AUTO_INCREMENT,
  movie_id BIGINT NOT NULL,
  cinema_id BIGINT NOT NULL,
  hall_id BIGINT NOT NULL,
  start_time DATETIME NOT NULL,
  end_time DATETIME NOT NULL,
  ticket_price DECIMAL(10,2) NOT NULL,
  language VARCHAR(32),
  version VARCHAR(32),
  schedule_status VARCHAR(16) NOT NULL DEFAULT 'ON_SALE',
  created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_schedule_movie FOREIGN KEY (movie_id) REFERENCES movie_basic(movie_id),
  CONSTRAINT fk_schedule_cinema FOREIGN KEY (cinema_id) REFERENCES cinema(cinema_id),
  CONSTRAINT fk_schedule_hall FOREIGN KEY (hall_id) REFERENCES hall(hall_id),
  KEY idx_schedule_movie (movie_id),
  KEY idx_schedule_start_time (start_time)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS ticket_order (
  order_id BIGINT PRIMARY KEY AUTO_INCREMENT,
  order_no VARCHAR(64) NOT NULL,
  user_id BIGINT NOT NULL,
  schedule_id BIGINT NOT NULL,
  seat_id BIGINT NOT NULL,
  payment_amount DECIMAL(10,2) NOT NULL,
  order_status VARCHAR(16) NOT NULL DEFAULT 'CREATED',
  order_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  paid_time DATETIME NULL,
  CONSTRAINT fk_ticket_order_user FOREIGN KEY (user_id) REFERENCES movie_user(user_id),
  CONSTRAINT fk_ticket_order_schedule FOREIGN KEY (schedule_id) REFERENCES schedule(schedule_id),
  CONSTRAINT fk_ticket_order_seat FOREIGN KEY (seat_id) REFERENCES seat(seat_id),
  UNIQUE KEY uk_ticket_order_no (order_no),
  KEY idx_ticket_order_user (user_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS payment_record (
  payment_id BIGINT PRIMARY KEY AUTO_INCREMENT,
  order_id BIGINT NOT NULL,
  payment_channel VARCHAR(32) NOT NULL,
  payment_amount DECIMAL(10,2) NOT NULL,
  payment_status VARCHAR(16) NOT NULL,
  transaction_no VARCHAR(64),
  payment_time DATETIME,
  created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_payment_record_order FOREIGN KEY (order_id) REFERENCES ticket_order(order_id),
  UNIQUE KEY uk_payment_record_transaction_no (transaction_no)
) ENGINE=InnoDB;
