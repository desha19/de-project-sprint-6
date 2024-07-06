--Шаг 7.3. Написать запрос и ответить на вопрос бизнеса.

with user_group_log as (
    select 
        luga.hk_group_id,
        count(distinct sah.user_id_from) as cnt_added_users
    from STV202406073__DWH.s_auth_history sah
    left join STV202406073__DWH.l_user_group_activity luga on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity 
    left join (
        select 
            hk_group_id
        from STV202406073__DWH.h_groups hg
        order by registration_dt desc
        limit 10
    ) hg on luga.hk_group_id = hg.hk_group_id
    where sah.event = 'add'
    group by luga.hk_group_id
),
user_group_messages as (
    select 
        luga.hk_group_id, 
        count(distinct luga.hk_user_id) as cnt_users_in_group_with_messages
    from STV202406073__DWH.l_user_group_activity luga 
    group by luga.hk_group_id
)
select 
    ug.hk_group_id,
    ug.cnt_added_users,
    um.cnt_users_in_group_with_messages,
    round((um.cnt_users_in_group_with_messages::numeric / ug.cnt_added_users), 2) as group_conversion
from user_group_log as ug
left join user_group_messages as um on ug.hk_group_id = um.hk_group_id
order by group_conversion desc;