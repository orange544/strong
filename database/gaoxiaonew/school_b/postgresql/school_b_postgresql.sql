SET client_encoding = 'UTF8';

DROP TABLE IF EXISTS ip_asset;
DROP TABLE IF EXISTS publication_record;
DROP TABLE IF EXISTS grant_award;

CREATE TABLE grant_award (
  grant_id VARCHAR(24) PRIMARY KEY,
  grant_name TEXT NOT NULL,
  grant_level VARCHAR(32) NOT NULL,
  funding_source VARCHAR(128) NOT NULL,
  leader_id VARCHAR(16) NOT NULL,
  arrival_fee NUMERIC(16,2) NOT NULL,
  status VARCHAR(32) NOT NULL
);

CREATE TABLE publication_record (
  publication_id VARCHAR(24) PRIMARY KEY,
  paper_title TEXT NOT NULL,
  journal_title VARCHAR(256) NOT NULL,
  include_type VARCHAR(32) NOT NULL,
  cited_times INT NOT NULL,
  publish_status VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL
);

CREATE TABLE ip_asset (
  ip_id VARCHAR(24) PRIMARY KEY,
  ip_name TEXT NOT NULL,
  ip_type VARCHAR(32) NOT NULL,
  conversion_income NUMERIC(16,2) NOT NULL,
  legal_status VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL
);
