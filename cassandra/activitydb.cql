DROP KEYSPACE IF EXISTS activitydb;

CREATE KEYSPACE activitydb
WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2};

USE activitydb;

CREATE TABLE user (
  user_id VARCHAR,
  name VARCHAR,
  zip VARCHAR,
  lat DOUBLE,
  lon DOUBLE,
  PRIMARY KEY (user_id)
);

CREATE TABLE activity_master (
  activity_id UUID,
  activity_type VARCHAR,
  user_id VARCHAR,
  name VARCHAR,
  time BIGINT,
  lat DOUBLE,
  lon DOUBLE,
  zip VARCHAR,
  PRIMARY KEY (activity_id)
);

CREATE TABLE activity_by_user (
  activity_type VARCHAR,
  zip VARCHAR,
  user_id VARCHAR,
  duration INT,
  lat DOUBLE,
  lon DOUBLE,
  PRIMARY KEY ((activity_type, zip), user_id)
);
