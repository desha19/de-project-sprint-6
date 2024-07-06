drop table if exists STV202406073__STAGING.group_log;

create table STV202406073__STAGING.group_log(
group_id int primary key,
user_id int,
user_id_from int,
event varchar(20),
"datetime" timestamp
)
ORDER BY group_id, user_id
SEGMENTED BY hash(group_id) all nodes
PARTITION BY "datetime"::date
GROUP BY calendar_hierarchy_day("datetime"::date, 3, 2);
COMMENT ON COLUMN STV202406073__STAGING.group_log.group_id IS 'уникальный идентификатор группы';
COMMENT ON COLUMN STV202406073__STAGING.group_log.user_id IS 'уникальный идентификатор пользователя';
COMMENT ON COLUMN STV202406073__STAGING.group_log.user_id_from IS 'поле для отметки о том, что пользователь не сам вступил в группу, а его добавил другой участник. Если пользователя пригласил в группу кто-то другой, поле будет непустым';
COMMENT ON COLUMN STV202406073__STAGING.group_log.event IS 'действие, которое совершено пользователем user_id';
COMMENT ON COLUMN STV202406073__STAGING.group_log."datetime" IS 'время совершения event';