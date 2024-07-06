SELECT count(g.admin_id)
FROM STV202406073__STAGING.groups g 
left join STV202406073__STAGING.users u on g.admin_id = u.id 
WHERE g.admin_id is null;