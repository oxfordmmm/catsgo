IF SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA 
WHERE SCHEMA_NAME = 'sp3' THEN
  SELECT 'Database sp3 already exists'
ELSE

  CREATE DATABASE `sp3`;
 
  -- Table creation
  CREATE TABLE `dirlist` (
    id int unsigned auto_increment primary key,
    watch_dir varchar(40)
  );

  CREATE TABLE `dirlist_dir` (
    id int unsigned auto_increment primary key,
    dirlist_id int,
    dir varchar(40)
  );

  CREATE TABLE `ignore_list` (
    id int unsigned auto_increment primary key,
    watch_dir varchar(40)
  );

  CREATE TABLE `ignore_list_dir` (
    id int unsigned auto_increment primary key,
    ignore_list_id int,
    dir varchar(40)
  );

  CREATE TABLE `metadata` (
    id int unsigned auto_increment primary key,
    catsup_uuid varchar(40),
    added_time varchar()
    apex_batch_status varchar(20)
    apex_batch_id varchar(50)
    apex_samples json,
    run_uuid varchar(40),
    submitted_metadata json,
  );

  CREATE TABLE `runlist` (
    id int unsigned auto_increment primary key,
    pipeline_name varchar(50)
  );

  CREATE TABLE `runlist_finished` (
    id int unsigned auto_increment primary key,
    runlist_id int,
    finished_uuids varchar(50)
  );

  -- Relationship creation
  ALTER TABLE `dirlist_dir` (
    ADD FOREIGN KEY (dirlist_id) references dirlist(id)
  );

  ALTER TABLE `ignore_list_dir` (
    ADD FOREIGN KEY (ignore_list_id) references ignore_list(id)
  );

  ALTER TABLE `runlist_finished` (
    ADD FOREIGN KEY (runlist_id) references runlist(id)
  );

END IF;
