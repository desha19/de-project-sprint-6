--Разработка аналитического хранилища. Задание 2.
--delete from STV202406073__DWH.h_users;
INSERT INTO STV202406073__DWH.h_users(hk_user_id, user_id, registration_dt, load_dt, load_src)
select
       hash(id) as  hk_user_id,
       id as user_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV202406073__STAGING.users
where hash(id) not in (select hk_user_id from STV202406073__DWH.h_users);
--
--delete from STV202406073__DWH.h_groups;
INSERT INTO STV202406073__DWH.h_groups(hk_group_id, group_id, registration_dt, load_dt, load_src)
select
       hash(id) as  hk_group_id,
       id as group_id,
       registration_dt as registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV202406073__STAGING.groups
where hash(id) not in (select hk_group_id from STV202406073__DWH.h_groups);
--
--delete from STV202406073__DWH.h_dialogs;
INSERT INTO STV202406073__DWH.h_dialogs(hk_message_id, message_id, message_ts, load_dt, load_src)
select
       hash(message_id) as hk_message_id,
       message_id as message_id,
       message_ts as message_ts,
       now() as load_dt,
       's3' as load_src
       from STV202406073__STAGING.dialogs
where hash(message_id) not in (select hk_message_id from STV202406073__DWH.h_dialogs);

--Втророй вариант от Руслана
INSERT INTO STV202406073__DWH.h_dialogs(hk_message_id, message_id,message_ts,load_dt,load_src)
select
       hash(message_id) as  hk_message_id,
       message_id,
       message_ts,
       now() as load_dt,
       's3' as load_src
       from STV202406073__STAGING.dialogs
where hash(message_id) not in (select hk_message_id from STV202406073__DWH.h_dialogs);