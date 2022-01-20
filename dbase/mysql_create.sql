  CREATE DATABASE IF NOT EXISTS `sp3`;
 
  -- Table creation
  CREATE TABLE IF NOT EXISTS `dirlist` (
    id int unsigned auto_increment primary key,
    watch_dir varchar(40)
  );

  CREATE TABLE IF NOT EXISTS `dirlist_dir` (
    id int unsigned auto_increment primary key,
    dirlist_id int,
    dir varchar(40),
    FOREIGN KEY (dirlist_id) references dirlist(id)
  );

  CREATE TABLE IF NOT EXISTS `ignore_list` (
    id int unsigned auto_increment primary key,
    watch_dir varchar(40)
  );

  CREATE TABLE IF NOT EXISTS `ignore_list_dir` (
    id int unsigned auto_increment primary key,
    ignore_list_id int,
    dir varchar(40),
    FOREIGN KEY (ignore_list_id) references ignore_list(id)
  );

  CREATE TABLE IF NOT EXISTS `metadata` (
    id int unsigned auto_increment primary key,
    catsup_uuid varchar(40),
    added_time varchar()
    apex_batch_status varchar(20)
    apex_batch_id varchar(50)
    apex_samples json,
    run_uuid varchar(40),
    submitted_metadata json,
  );

  CREATE TABLE IF NOT EXISTS `runlist` (
    id int unsigned auto_increment primary key,
    pipeline_name varchar(50)
  );

  CREATE TABLE IF NOT EXISTS `runlist_finished` (
    id int unsigned auto_increment primary key,
    runlist_id int,
    finished_uuids varchar(50),
    FOREIGN KEY (runlist_id) references runlist(id)
  );

END IF;
