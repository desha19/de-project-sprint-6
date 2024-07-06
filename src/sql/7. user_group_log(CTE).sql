--Шаг 7.2. Подготовить CTE user_group_log

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
)
select hk_group_id, cnt_added_users
from user_group_log
order by cnt_added_users asc
limit 10;