--Шаг 7.1. Подготовить CTE user_group_messages.

with user_group_messages as (
    select 
    	luga.hk_group_id, 
    	count(distinct luga.hk_user_id) as cnt_users_in_group_with_messages
    from STV202406073__DWH.l_user_group_activity luga 
    group by luga.hk_group_id
)
select hk_group_id,
       cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages asc
limit 10;