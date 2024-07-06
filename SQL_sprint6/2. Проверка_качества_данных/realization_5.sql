(SELECT 
    max(u.registration_dt) < now() as 'no future dates',
    '2020-Sep-03' < min(u.registration_dt) as 'no false-start dates',
    'users' as dataset
FROM STV202406073__STAGING.users u)
UNION ALL
(SELECT
    max(g.registration_dt) < now() as 'no future dates',
    '2020-Sep-03' < min(g.registration_dt) as 'no false-start dates',
    'groups' as dataset
FROM STV202406073__STAGING.groups g)
UNION ALL
(SELECT
    max(d.message_ts) < now() as 'no future dates',
    '2020-Sep-03' < min(d.message_ts) as 'no false-start dates',
    'dialogs' as dataset
FROM STV202406073__STAGING.dialogs d);