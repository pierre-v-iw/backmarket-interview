SELECT 
	*
FROM
	`fabled-archive-306817.backmarket.catalog_raw`
WHERE
	1=1
	AND (image IS NULL OR image = '')
	AND year_release > 2000
	AND brand = 'Samsung'