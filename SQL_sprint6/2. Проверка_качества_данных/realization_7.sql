(SELECT count(1), 'missing group admin info' as info
FROM STV202406073__STAGING.groups g
left join STV202406073__STAGING.users u on u.id = g.admin_id
WHERE u.id is null)
UNION ALL
(SELECT COUNT(1), 'missing sender info'
FROM STV202406073__STAGING.dialogs d 
left join STV202406073__STAGING.users u2 on d.message_from = u2.id 
WHERE u2.id is null)
UNION ALL
(SELECT COUNT(1), 'missing receiver info'
FROM STV202406073__STAGING.dialogs d
left join STV202406073__STAGING.users u3 on d.message_to = u3.id 
WHERE u3.id is null)
UNION ALL 
(SELECT COUNT(1), 'norm receiver info'
FROM STV202406073__STAGING.dialogs d
left join STV202406073__STAGING.users u4 on d.message_to = u4.id 
WHERE u4.id is not null);