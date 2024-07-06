(SELECT 
    min(u.registration_dt) as datestamp,
    'earliest user registration' as info
FROM STV202406073__STAGING.users u)
UNION ALL
(SELECT
    max(u.registration_dt),
    'latest user registration'
FROM STV202406073__STAGING.users u)
--
UNION ALL
--
(SELECT 
    min(g.registration_dt) as datestamp,
    'earliest group registration' as info
FROM STV202406073__STAGING.groups g)
UNION ALL
(SELECT
    max(g.registration_dt),
    'latest group registration'
FROM STV202406073__STAGING.groups g)
--
UNION ALL
--
(SELECT 
    min(d.message_ts) as datestamp,
    'earliest dialog registration' as info
FROM STV202406073__STAGING.dialogs d)
UNION ALL
(SELECT
    max(d.message_ts),
    'latest dialog registration'
FROM STV202406073__STAGING.dialogs d);