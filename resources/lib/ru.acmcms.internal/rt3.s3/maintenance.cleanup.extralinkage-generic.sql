DELETE FROM s3ExtraLink WHERE recId IN (
	SELECT l.recId 
	FROM s3ExtraLink l 
	LEFT OUTER JOIN s3Extra e USING (recId) 
	LEFT OUTER JOIN s3Objects o USING (objId) 
	WHERE o.objCreated is NULL OR e.recId is NULL GROUP BY l.recId
);
