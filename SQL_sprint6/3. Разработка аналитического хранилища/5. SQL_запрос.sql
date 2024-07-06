--Разработка аналитического хранилища. Задание 5.

drop table if exists STV202406073__DWH.s_admins;

create table STV202406073__DWH.s_admins
(
	hk_admin_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES STV202406073__DWH.l_admins (hk_l_admin_id),
	is_admin boolean,
	admin_from datetime,
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO STV202406073__DWH.s_admins(hk_admin_id, is_admin,admin_from,load_dt,load_src)
select la.hk_l_admin_id,
True as is_admin,
hg.registration_dt,
now() as load_dt,
's3' as load_src
from STV202406073__DWH.l_admins as la
left join STV202406073__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;
------------------------------------------------------------------------------
--s_group_name
drop table if exists STV202406073__DWH.s_group_name;

create table STV202406073__DWH.s_group_name
(
	hk_group_id bigint not null CONSTRAINT fk_s_group_name_h_groups REFERENCES STV202406073__DWH.h_groups (hk_group_id),
	group_name varchar(100),
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV202406073__DWH.s_group_name(hk_group_id, group_name, load_dt, load_src)
select 
hg.hk_group_id,
g.group_name,
now() as load_dt,
's3' as load_src
from STV202406073__DWH.h_groups hg
left join STV202406073__STAGING.groups g on hg.group_id = g.id;
------------------------------------------------------------------------------
--s_group_private_status
drop table if exists STV202406073__DWH.s_group_private_status;

create table STV202406073__DWH.s_group_private_status
(
	hk_group_id bigint not null CONSTRAINT fk_s_group_private_status_h_groups REFERENCES STV202406073__DWH.h_groups (hk_group_id),
	is_private boolean,
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV202406073__DWH.s_group_private_status(hk_group_id, is_private, load_dt, load_src)
select
hg2.hk_group_id,
g2.is_private,
now() as load_dt,
's3' as load_src
from STV202406073__DWH.h_groups hg2
left join STV202406073__STAGING.groups g2 on hg2.group_id = g2.id;
------------------------------------------------------------------------------
--s_dialog_info
drop table if exists STV202406073__DWH.s_dialog_info;

create table STV202406073__DWH.s_dialog_info
(
	hk_message_id bigint not null CONSTRAINT fk_s_dialog_info_h_dialogs REFERENCES STV202406073__DWH.h_dialogs (hk_message_id),
	message varchar(1000),
	message_from integer,
	message_to integer,
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV202406073__DWH.s_dialog_info(hk_message_id, message, message_from, message_to, load_dt, load_src)
select 
hd.hk_message_id,
d.message,
d.message_from,
d.message_to,
now() as load_dt,
's3' as load_src
from STV202406073__DWH.h_dialogs hd
left join STV202406073__STAGING.dialogs d on hd.message_id = d.message_id;
------------------------------------------------------------------------------
--s_user_socdem
drop table if exists STV202406073__DWH.s_user_socdem;

create table STV202406073__DWH.s_user_socdem
(
	hk_user_id bigint not null CONSTRAINT fk_s_user_socdem_h_users REFERENCES STV202406073__DWH.h_users (hk_user_id),
	country varchar(200),
	age integer,
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

--delete from STV202406073__DWH.s_user_socdem;
INSERT INTO STV202406073__DWH.s_user_socdem(hk_user_id, country, age, load_dt, load_src)
select 
hu.hk_user_id,
u.country,
u.age,
now() as load_dt,
's3' as load_src
from STV202406073__DWH.h_users hu
left join STV202406073__STAGING.users u on hu.user_id = u.id; 
------------------------------------------------------------------------------
--s_user_chatinfo
drop table if exists STV202406073__DWH.s_user_chatinfo;

create table STV202406073__DWH.s_user_chatinfo
(
	hk_user_id bigint not null CONSTRAINT fk_s_user_chatinfo_h_users REFERENCES STV202406073__DWH.h_users (hk_user_id),
	chat_name varchar(200),
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);