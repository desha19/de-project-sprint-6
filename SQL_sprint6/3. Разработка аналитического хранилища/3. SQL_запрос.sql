--Разработка аналитического хранилища. Задание 3.

drop table if exists STV202406073__DWH.l_user_message;
drop table if exists STV202406073__DWH.l_admins;
drop table if exists STV202406073__DWH.l_groups_dialogs;

create table STV202406073__DWH.l_user_message
(
	hk_l_user_message bigint primary key,
	hk_user_id bigint not null CONSTRAINT fk_l_user_message_user REFERENCES STV202406073__DWH.h_users (hk_user_id),
	hk_message_id bigint not null CONSTRAINT fk_l_user_message REFERENCES STV202406073__DWH.h_dialogs (hk_message_id),
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
--
create table STV202406073__DWH.l_admins
(
	hk_l_admin_id bigint primary key,
	hk_user_id bigint not null CONSTRAINT fk_l_admins REFERENCES STV202406073__DWH.h_users (hk_user_id),
	hk_group_id bigint not null CONSTRAINT fk_l_admins_groups REFERENCES STV202406073__DWH.h_groups (hk_group_id),
	load_dt datetime,
	load_src varchar(20) 
)
order by load_dt
SEGMENTED BY hk_l_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
--
create table STV202406073__DWH.l_groups_dialogs
(
	hk_l_groups_dialogs bigint primary key,
	hk_message_id bigint not null CONSTRAINT fk_l_groups_dialogs REFERENCES STV202406073__DWH.h_dialogs (hk_message_id),
	hk_group_id bigint not null CONSTRAINT fk_l_groups_dialogs_dialogs REFERENCES STV202406073__DWH.h_groups (hk_group_id),
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_groups_dialogs all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);