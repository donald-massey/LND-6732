IF OBJECT_ID(N'countyScansTitle.dbo.LND_6732_SRC_20250414') IS NOT NULL
	DROP TABLE countyScansTitle.dbo.LND_6732_SRC_20250414;

-- Count 2,081,043
SELECT tel.leaseid, tel.recordid, CONCAT(LOWER(tls.StateAbbreviation), '/', LOWER(tlc.CountyName)) AS state_countyname, tlicr.package_id,
'                                                                                                                       ' AS source_path,
'                                                                                                                  ' AS destination_path,
0                                                                                                                          AS page_count,
0                                                                                                                           AS file_size,
'                                                                                                                           '  AS status
INTO countyScansTitle.dbo.LND_6732_SRC_20250414
FROM countyScansTitle.dbo.tblrecord tr
LEFT JOIN countyScansTitle.dbo.tblexportLog tel ON tr.recordID = tel.recordID
LEFT JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] tll ON tll.leaseID = tel.leaseID
LEFT JOIN CS_Digital.dbo.tblLeaseIDxref tlicr ON tlicr.lease_id = tel.LeaseID
LEFT JOIN countyScansTitle.dbo.tbllookupCounties tlc ON tr.countyID = tlc.CountyID
LEFT JOIN countyScansTitle.dbo.tbllookupStates tls ON tr.stateID = tls.StateID
WHERE CONVERT(varchar, tr.receivedDate, 23) = '2025-04-07'
AND package_id IS NOT NULL
ORDER BY tel.leaseid

IF OBJECT_ID(N'countyScansTitle.dbo.LND_6732_DEST_20250414') IS NOT NULL
	DROP TABLE countyScansTitle.dbo.LND_6732_DEST_20250414;

-- Count 2,080,923
-- Investigated the 120 
SELECT COUNT(*)
FROM countyScansTitle.dbo.LND_6732_DEST_20250414 WITH(NOLOCK)


/*
DELETE FROM countyScansTitle.dbo.LND_6732_SRC_20250414
WHERE EXISTS (
    SELECT 1
    FROM countyScansTitle.dbo.LND_6732_DEST_20250414
    WHERE countyScansTitle.dbo.LND_6732_SRC_20250414.recordID = countyScansTitle.dbo.LND_6732_DEST_20250414.recordID
);
*/

/*
DELETE FROM countyScansTitle.dbo.LND_6732_DEST_20250414
WHERE status != 'processed';
*/

/*
DELETE FROM countyScansTitle.dbo.LND_6732_SRC_20250414
WHERE leaseid in (SELECT leaseid from countyScansTitle.dbo.LND_6732_DEST_20250414)
*/

SELECT *
FROM countyScansTitle.dbo.LND_6732_DEST_20250414 WITH(NOLOCK)
WHERE status != 'processed'
WHERE page_count = 0

SELECT *
FROM countyScansTitle.dbo.LND_6732_SRC_20250414 WITH(NOLOCK)


SELECT COUNT(*)
FROM countyScansTitle.dbo.LND_6732_SRC_20250414

-- Insert records from SourceTable to DestinationTable
INSERT INTO countyScansTitle.dbo.LND_6732_SRC_20250414 (leaseid, recordid, state_countyname, package_id, source_path, destination_path, page_count, file_size, status)
SELECT leaseid, recordid, state_countyname, package_id, source_path, destination_path, page_count, file_size, ''
FROM countyScansTitle.dbo.LND_6732_DEST_20250414
WHERE status != 'processed'


-- Start creating the process to copy the images over to the new bucket and update tblS3Image