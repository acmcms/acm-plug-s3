SELECT
(SELECT COUNT(*) FROM s3ChangeInfo) as change,
(SELECT COUNT(*) FROM (SELECT e.recId FROM s3Extra e LEFT OUTER JOIN s3ExtraLink l ON e.recId=l.recId WHERE  l.recId IS NULL GROUP BY e.recId) as dead) as dead,
(SELECT COUNT(*) FROM (SELECT l.objId FROM s3ExtraLink l LEFT OUTER JOIN s3Objects o ON l.objId=o.objId LEFT OUTER JOIN s3ObjectHistory h ON l.objId=h.hsId LEFT OUTER JOIN s3ObjectVersions v ON l.objId=v.vrId LEFT OUTER JOIN s3Extra e ON l.recId=e.recId WHERE o.objId is NULL AND h.hsId is NULL AND v.vrId is NULL GROUP BY l.objId) as lost) as lost