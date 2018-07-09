CREATE TEMPORARY TABLE tmptable SELECT code FROM s3Dictionary LEFT JOIN s3Indices USING(code) WHERE luid IS NULL;
CREATE INDEX idx_code ON tmptable (code);
DELETE FROM s3Dictionary WHERE code IN (SELECT code FROM tmptable);
DROP TABLE tmptable;
