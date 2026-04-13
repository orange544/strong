CREATE DATABASE IF NOT EXISTS gaoxiao_db15_library
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE gaoxiao_db15_library;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS fine_payment;
DROP TABLE IF EXISTS overdue_record;
DROP TABLE IF EXISTS reservation_record;
DROP TABLE IF EXISTS return_record;
DROP TABLE IF EXISTS borrow_record;
DROP TABLE IF EXISTS book_copy;
DROP TABLE IF EXISTS book_info;
DROP TABLE IF EXISTS reader_master;

CREATE TABLE reader_master (
  reader_id VARCHAR(32) NOT NULL,
  reader_code VARCHAR(32) NOT NULL,
  reader_name VARCHAR(50) NOT NULL,
  sex VARCHAR(10) NOT NULL,
  age TINYINT UNSIGNED NOT NULL,
  cert_no VARCHAR(18) NOT NULL,
  mobile_no VARCHAR(20) NOT NULL,
  reader_type VARCHAR(20) NOT NULL,
  dept_id VARCHAR(20) NOT NULL,
  reader_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (reader_id),
  UNIQUE KEY uk_reader_code (reader_code),
  UNIQUE KEY uk_reader_cert_no (cert_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE book_info (
  book_id VARCHAR(32) NOT NULL,
  isbn VARCHAR(20) NOT NULL,
  book_title VARCHAR(200) NOT NULL,
  author_name VARCHAR(100) NOT NULL,
  publisher VARCHAR(100) NOT NULL,
  pub_year INT NOT NULL,
  category_code VARCHAR(20) NOT NULL,
  status VARCHAR(20) NOT NULL,
  PRIMARY KEY (book_id),
  UNIQUE KEY uk_book_isbn (isbn)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE book_copy (
  copy_id VARCHAR(32) NOT NULL,
  book_id VARCHAR(32) NOT NULL,
  barcode VARCHAR(40) NOT NULL,
  shelf_location VARCHAR(40) NOT NULL,
  copy_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (copy_id),
  UNIQUE KEY uk_copy_barcode (barcode),
  KEY idx_copy_book (book_id),
  CONSTRAINT fk_copy_book
    FOREIGN KEY (book_id) REFERENCES book_info(book_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE borrow_record (
  borrow_id VARCHAR(32) NOT NULL,
  reader_id VARCHAR(32) NOT NULL,
  copy_id VARCHAR(32) NOT NULL,
  borrow_time DATETIME NOT NULL,
  due_date DATE NOT NULL,
  borrow_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (borrow_id),
  KEY idx_borrow_reader (reader_id),
  KEY idx_borrow_copy (copy_id),
  CONSTRAINT fk_borrow_reader
    FOREIGN KEY (reader_id) REFERENCES reader_master(reader_id),
  CONSTRAINT fk_borrow_copy
    FOREIGN KEY (copy_id) REFERENCES book_copy(copy_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE return_record (
  return_id VARCHAR(32) NOT NULL,
  borrow_id VARCHAR(32) NOT NULL,
  return_time DATETIME NOT NULL,
  return_condition VARCHAR(40) NOT NULL,
  operator_name VARCHAR(50) NOT NULL,
  PRIMARY KEY (return_id),
  UNIQUE KEY uk_return_borrow (borrow_id),
  CONSTRAINT fk_return_borrow
    FOREIGN KEY (borrow_id) REFERENCES borrow_record(borrow_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE reservation_record (
  reserve_id VARCHAR(32) NOT NULL,
  reader_id VARCHAR(32) NOT NULL,
  book_id VARCHAR(32) NOT NULL,
  reserve_time DATETIME NOT NULL,
  reserve_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (reserve_id),
  KEY idx_reserve_reader (reader_id),
  KEY idx_reserve_book (book_id),
  CONSTRAINT fk_reserve_reader
    FOREIGN KEY (reader_id) REFERENCES reader_master(reader_id),
  CONSTRAINT fk_reserve_book
    FOREIGN KEY (book_id) REFERENCES book_info(book_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE overdue_record (
  overdue_id VARCHAR(32) NOT NULL,
  borrow_id VARCHAR(32) NOT NULL,
  overdue_days INT NOT NULL,
  fine_amount DECIMAL(10,2) NOT NULL,
  process_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (overdue_id),
  KEY idx_overdue_borrow (borrow_id),
  CONSTRAINT fk_overdue_borrow
    FOREIGN KEY (borrow_id) REFERENCES borrow_record(borrow_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE fine_payment (
  payment_id VARCHAR(32) NOT NULL,
  overdue_id VARCHAR(32) NOT NULL,
  reader_id VARCHAR(32) NOT NULL,
  pay_amount DECIMAL(10,2) NOT NULL,
  pay_time DATETIME NOT NULL,
  payment_status VARCHAR(20) NOT NULL,
  voucher_no VARCHAR(40) NOT NULL,
  PRIMARY KEY (payment_id),
  KEY idx_payment_overdue (overdue_id),
  KEY idx_payment_reader (reader_id),
  CONSTRAINT fk_payment_overdue
    FOREIGN KEY (overdue_id) REFERENCES overdue_record(overdue_id),
  CONSTRAINT fk_payment_reader
    FOREIGN KEY (reader_id) REFERENCES reader_master(reader_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;
