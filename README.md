create external table track_info(
  ip string,
  country string,
  province string,
  city string,
  url string,
  time string,
  page string
) partitioned by (day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
location '/project/trackinfo/';

LOAD DATA INPATH 'hdfs://hadoop000:8020/project/input/etl/' OVERWRITE INTO TABLE track_info partition(day='2013-07-21');

select count(*) from track_info where day='2013-07-21';

select province, count(*) as cnt from track_info where day='2013-07-21' group by province;

// province stat table.
create table track_info_province_stat(
  province string,
  cnt bigint
) partitioned by (day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

insert overwrite table track_info_province_stat partition(day='2013-07-21') select province, count(*) as cnt from track_info where day='2013-07-21' group by province;

