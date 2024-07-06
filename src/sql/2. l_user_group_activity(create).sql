--Шаг 4. Создать таблицу связи.

drop table if exists STV202406073__STAGING.l_user_group_activity;

create table STV202406073__DWH.l_user_group_activity(
	hk_l_user_group_activity bigint primary key,
	hk_user_id bigint not null CONSTRAINT fk_l_user_group_activity_h_users REFERENCES STV202406073__DWH.h_users (hk_user_id),
	hk_group_id bigint not null CONSTRAINT fk_l_user_group_activity_h_groups REFERENCES STV202406073__DWH.h_groups (hk_group_id),
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);