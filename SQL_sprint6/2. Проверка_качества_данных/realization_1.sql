select count(id) as кол_id, 
	   count(distinct id) as кол_уник_id
from STV202406073__STAGING.users u;