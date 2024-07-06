--Создадим staging в Vertica. Задание 1.

drop table if exists STV202406073__STAGING.users;
drop table if exists STV202406073__STAGING.groups;
drop table if exists STV202406073__STAGING.dialogs;

create table STV202406073__STAGING.users(
id int primary key,
сhat_name varchar(200),
registration_dt timestamp,
country varchar(200),
age int
)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES;
;
--
create table STV202406073__STAGING.groups(
id int primary key,
admin_id int,
group_name varchar(200),
registration_dt timestamp,
is_private boolean
)
ORDER BY id, admin_id
SEGMENTED BY hash(id) all nodes
PARTITION BY registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2)
;
--
create table STV202406073__STAGING.dialogs(
message_id int primary key,
message_ts timestamp,
message_from int,
message_to int,
message varchar(1000),
message_group int
)
ORDER BY message_id
SEGMENTED BY hash(message_id) all nodes
PARTITION BY message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2)
;