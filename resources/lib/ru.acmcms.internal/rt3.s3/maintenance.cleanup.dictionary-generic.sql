DELETE 
FROM s3Dictionary 
WHERE code in (
	SELECT code FROM s3Dictionary LEFT OUTER JOIN s3Indices USING(code) WHERE luid IS NULL
);