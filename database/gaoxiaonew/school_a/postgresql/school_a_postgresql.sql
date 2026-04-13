SET client_encoding = 'UTF8';

DROP TABLE IF EXISTS patent_record;
DROP TABLE IF EXISTS paper_output;
DROP TABLE IF EXISTS research_project;

CREATE TABLE research_project (
  project_id VARCHAR(24) PRIMARY KEY,
  project_name TEXT NOT NULL,
  project_code VARCHAR(64) NOT NULL UNIQUE,
  project_level VARCHAR(32) NOT NULL,
  project_source VARCHAR(128) NOT NULL,
  principal_id VARCHAR(16) NOT NULL,
  received_amount NUMERIC(16,2) NOT NULL,
  status VARCHAR(32) NOT NULL
);

CREATE TABLE paper_output (
  paper_id VARCHAR(24) PRIMARY KEY,
  title TEXT NOT NULL,
  journal_name VARCHAR(256) NOT NULL,
  index_type VARCHAR(32) NOT NULL,
  doi VARCHAR(128) NOT NULL UNIQUE,
  citation_count INT NOT NULL,
  paper_status VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL
);

CREATE TABLE patent_record (
  patent_id VARCHAR(24) PRIMARY KEY,
  patent_name TEXT NOT NULL,
  patent_type VARCHAR(32) NOT NULL,
  application_no VARCHAR(64) NOT NULL UNIQUE,
  transfer_amount NUMERIC(16,2) NOT NULL,
  current_status VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL
);
