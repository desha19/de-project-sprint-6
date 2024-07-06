select count(id) as total, 
	   count(distinct id) as uniq,
	   'users' as dataset
from STV202406073__STAGING.users u 
UNION ALL
select count(id) as total,
	   count(distinct id) as uniq,
	   'groups' as dataset
from STV202406073__STAGING.groups g 
UNION ALL
select count(message_id) as total,
	   count(distinct message_id) as unic,
	   'dialogs' as dataset
from STV202406073__STAGING.dialogs d;