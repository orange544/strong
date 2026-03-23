// Neo4j schema for movie domain (constraints and indexes only).
// Canonical node labels: Movie, Person, User, Genre, Company, Country, Award
// Canonical relationship types: ACTED_IN, DIRECTED_BY, BELONGS_TO, PRODUCED_BY, RELEASED_IN, WON_AWARD, RATED, COMMENTED

CREATE CONSTRAINT movie_movie_id_unique IF NOT EXISTS
FOR (m:Movie)
REQUIRE m.movie_id IS UNIQUE;

CREATE CONSTRAINT person_person_id_unique IF NOT EXISTS
FOR (p:Person)
REQUIRE p.person_id IS UNIQUE;

CREATE CONSTRAINT user_user_md5_unique IF NOT EXISTS
FOR (u:User)
REQUIRE u.user_md5 IS UNIQUE;

CREATE CONSTRAINT company_company_id_unique IF NOT EXISTS
FOR (c:Company)
REQUIRE c.company_id IS UNIQUE;

CREATE CONSTRAINT award_award_id_unique IF NOT EXISTS
FOR (a:Award)
REQUIRE a.award_id IS UNIQUE;

CREATE CONSTRAINT genre_name_unique IF NOT EXISTS
FOR (g:Genre)
REQUIRE g.name IS UNIQUE;

CREATE CONSTRAINT country_name_unique IF NOT EXISTS
FOR (c:Country)
REQUIRE c.name IS UNIQUE;

CREATE INDEX movie_name_index IF NOT EXISTS
FOR (m:Movie)
ON (m.name);

CREATE INDEX movie_release_date_index IF NOT EXISTS
FOR (m:Movie)
ON (m.release_date);

CREATE INDEX movie_imdb_id_index IF NOT EXISTS
FOR (m:Movie)
ON (m.imdb_id);

CREATE INDEX person_name_index IF NOT EXISTS
FOR (p:Person)
ON (p.name);

CREATE INDEX user_nickname_index IF NOT EXISTS
FOR (u:User)
ON (u.user_nickname);
