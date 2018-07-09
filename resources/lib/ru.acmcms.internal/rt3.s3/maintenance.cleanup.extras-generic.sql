DELETE FROM s3Extra WHERE recId IN (
	SELECT e.recId FROM s3Extra e LEFT OUTER JOIN s3ExtraLink l ON e.recId=l.recId WHERE l.recId IS NULL GROUP BY e.recId
);
