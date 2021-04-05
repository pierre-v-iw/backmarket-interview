SELECT 
	*
FROM
	`{{table_id}}`
WHERE
	1=1
	AND image IS NOT NULL 
	AND image != ''