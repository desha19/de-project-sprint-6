SELECT count(hash(g.group_name)),
	   count(g.group_name) 
FROM STV202406073__STAGING.groups g;