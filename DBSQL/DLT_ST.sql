CREATE STREAMING TABLE iot_device_stream_rtumati
(
  user_id LONG,
  calories_burnt FLOAT,
  num_steps LONG,
  miles_walked FLOAT,
  time_stamp TIMESTAMP,
  device_id LONG
) 
COMMENT "streaming table for IOT Device"
AS
SELECT
  CAST(user_id AS LONG) AS user_id,
  CAST(calories_burnt AS FLOAT) AS calories_burnt,
  CAST(num_steps AS LONG) AS num_steps,
  CAST(miles_walked AS FLOAT) AS miles_walked,
  CAST(timestamp AS TIMESTAMP) AS time_stamp,
  CAST(device_id AS LONG) AS device_id
FROM STREAM read_files("dbfs:/databricks-datasets/iot-stream/data-device/*.gz");


CREATE STREAMING TABLE users
COMMENT "streaming table for IOT Device"
AS
SELECT
  CAST(userid AS INTEGER) AS user_id,
  gender,
  CAST(age AS INTEGER) AS age,
  CAST(height AS INTEGER) AS height,
  CAST(weight AS INTEGER) AS weight,
  smoker,
  familyhistory,
  cholestlevs,
  bp,
  CAST(risk AS INTEGER) AS risk
FROM STREAM read_files("dbfs:/databricks-datasets/iot-stream/data-user/");


CREATE MATERIALIZED VIEW user_activity AS
select 
i.time_stamp,
u.risk,
u.age,
avg(i.colories_burnt) as avg_colories_burnt,
count(*) as num_records,
AVG(i.num_steps) as avg_num_steps,
AVG(i.miles_walked) as avg_miles_walked
FROM rtumati.iot_device_stream_rtumati i
join rtumati.users u
on i.user_id = u.user_id
group by ALL;

