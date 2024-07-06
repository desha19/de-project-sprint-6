--Разработка аналитического хранилища. Задание 4.

--delete from STV202406073__DWH.l_admins;
INSERT INTO STV202406073__DWH.l_admins(hk_l_admin_id, hk_group_id,hk_user_id,load_dt,load_src)
select
hash(hg.hk_group_id, hu.hk_user_id),
hg.hk_group_id,
hu.hk_user_id,
now() as load_dt,
's3' as load_src
from STV202406073__STAGING.groups as g
left join STV202406073__DWH.h_users as hu on g.admin_id = hu.user_id
left join STV202406073__DWH.h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_admin_id from STV202406073__DWH.l_admins);
--
--delete from STV202406073__DWH.l_groups_dialogs;
INSERT INTO STV202406073__DWH.l_groups_dialogs(hk_l_groups_dialogs, hk_message_id, hk_group_id, load_dt, load_src)
select 
hash(hg.hk_group_id, hd.hk_message_id),
hd.hk_message_id,
hg.hk_group_id,
now() as load_dt,
's3' as load_src
from STV202406073__STAGING.dialogs d
left join STV202406073__DWH.h_dialogs hd on d.message_id = hd.message_id 
left join STV202406073__DWH.h_groups hg on d.message_group = hg.group_id 
where hash(hg.hk_group_id, hd.hk_message_id) not in (select hk_l_groups_dialogs from STV202406073__DWH.l_groups_dialogs) and
	  d.message_group is not null;