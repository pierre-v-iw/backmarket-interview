SELECT 
	*
FROM
	`{{table_id}}`
WHERE
	1=1
	AND (image IS NULL OR image = '')
	AND year_release > 2000
	AND brand = 'Samsung'