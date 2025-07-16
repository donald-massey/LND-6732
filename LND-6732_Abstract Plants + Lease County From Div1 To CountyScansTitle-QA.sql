/*========================================================================
SQL QA QUERIES - MISSING RECORDS ANALYSIS FOR tblLegalLease
========================================================================*/
-- Records missing from tblexportLog, tblrecord available in tblLegalLease
SELECT 
    ll.updated, ll.*
FROM 
    [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] ll
LEFT JOIN (
    SELECT 
        tel.leaseID 
    FROM 
        countyScansTitle.dbo.tblrecord tr
    JOIN 
        countyScansTitle.dbo.tblexportLog tel 
        ON tr.recordID = tel.recordID
) joined_records
ON ll.leaseID = joined_records.leaseID
WHERE 
    joined_records.leaseID IS NULL
ORDER BY ll.updated ASC;


-- tblrecord Total Count
-- 6,489,039 Records
SELECT 
    COUNT(*) AS tblrecord_count
FROM 
    countyScansTitle.dbo.tblrecord
WHERE InstrumentTypeID IN ('MOGL','OGL','OGLAMD','POGL','OGLEXT','ROGL') 
AND statusID IN (4,10,16,18,90)
AND recordIsLease = 1

-- tblrecord Upate Count
-- 3,750,972 Records
SELECT 
    COUNT(*) AS tblrecord_update_count
FROM 
    countyScansTitle.dbo.tblrecord
WHERE remarks LIKE '%LND-6732%'

-- tblexportLog Total Count
-- 31,568,848 Records
SELECT 
    COUNT(DISTINCT recordID) AS tblexportlog_count
FROM 
    countyScansTitle.dbo.tblrecord

-- tblexportlog Update Count
-- 3,750,972 Records
SELECT 
    COUNT(DISTINCT recordID) AS tblexportlog_count
FROM 
    countyScansTitle.dbo.tblrecord
WHERE remarks LIKE '%LND-6732%'



/*========================================================================
SQL QA QUERIES - MISSING RECORDS ANALYSIS FOR tblGrantorGrantee
========================================================================*/
SELECT 
    gg.recordID AS Missing_RecordID,
    COUNT(*) AS Count_Of_Entries
FROM 
    countyScansTitle.dbo.LND_6833_tblgrantorGrantee gg
LEFT JOIN 
    countyScansTitle.dbo.tblrecord r ON gg.recordID = r.recordID
WHERE 
    r.recordID IS NULL
GROUP BY 
    gg.recordID
ORDER BY 
    Count_Of_Entries DESC;


-- tblgrantorGrantee Total Records
-- 88,931,200
SELECT COUNT(DISTINCT recordID) AS tblgrantorgrantee_total_count
FROM countyScansTitle.dbo.tblgrantorGrantee

-- LND_6833_tblgrantorGrantee Update Records
-- 3,911,697
SELECT COUNT(*) AS LND_6833_tblgrantorGrantee_update_count
FROM countyScansTitle.dbo.LND_6833_tblgrantorGrantee
WHERE recordType = 'Grantor'

-- 3,810,407
SELECT COUNT(*) AS LND_6833_tblgrantorGrantee_update_count
FROM countyScansTitle.dbo.LND_6833_tblgrantorGrantee
WHERE recordType = 'Grantee'



/*========================================================================
SQL QA QUERIES - MISSING RECORDS ANALYSIS FOR tbldeedReferenceVolumePage
========================================================================*/
SELECT 
    drvp.recordID AS Missing_RecordID,
    COUNT(*) AS Count_Of_Entries
FROM 
    countyScansTitle.dbo.LND_6836_tbldeedReferenceVolumePage drvp
LEFT JOIN 
    countyScansTitle.dbo.tblrecord r ON drvp.recordID = r.recordID
WHERE 
    r.recordID IS NULL
GROUP BY 
    drvp.recordID
ORDER BY 
    Count_Of_Entries DESC;

SELECT 
    COUNT(DISTINCT recordID) AS tbldeedReferenceVolumePage_total_count
FROM 
    countyScansTitle.dbo.tbldeedReferenceVolumePage;

SELECT 
    COUNT(*) AS LND_6836_tbldeedReferenceVolumePage_update_count
FROM 
    countyScansTitle.dbo.LND_6836_tbldeedReferenceVolumePage;


/*========================================================================
SQL QA QUERIES - MISSING RECORDS ANALYSIS FOR tbllandDescription
========================================================================*/
SELECT 
    ld.recordID AS Missing_RecordID,
    COUNT(*) AS Count_Of_Entries
FROM 
    countyScansTitle.dbo.LND_6838_tbllandDescription ld
LEFT JOIN 
    countyScansTitle.dbo.tblrecord r ON ld.recordID = r.recordID
WHERE 
    r.recordID IS NULL
GROUP BY 
    ld.recordID
ORDER BY 
    Count_Of_Entries DESC;

SELECT 
    COUNT(DISTINCT recordID) AS tbllandDescription_total_count
FROM 
    countyScansTitle.dbo.tbllandDescription;

SELECT 
    COUNT(*) AS LND_6838_tbllandDescription_update_count
FROM 
    countyScansTitle.dbo.LND_6838_tbllandDescription;




-- Add query to show Lindsey recordIsLease = 1 & recordIsCourthouse = 0
/* 
-- Final Steps
1. create a tidy QA query file
	a. There was 1-2 good queries from the chatgpt, reduce it to those
	
	add the answers to What Changed, Is Anything Left To Be Changed
	Did I solve the problem as intended? 
		do the attributes make sense in the fields mapped to

	Did I apply the solution to the right records? 
		did i miss any are the counts overall in target range/make sense?

	Did I apply the solution to records I shouldn't have?
		did i over select what to run on

2. create a summary with counts of what was done
	1. record counts and categories with explanation

3. review deploy plan one more time
	talk with Ales and Tyler about what all to verify/additional tests
	lease = 1
	courthouse = 0

4. deploy

5. verify in production
	Records are looking right as expected
	don't get re-exported into div1
	ch lease exporter
	ch database exports
	land lease producer should not reproduce all of these at once

6. test a couple records with workflow we plan for LDI
	1. make sure changes get picked up by land lease producer after they move it out of status 90

7. communication with LDI as to new workflow

8. Come back for land descriptions for Marcellus

9. communication with LDI

10. communicate with stakeholders as to this being done
	1. deprecate Lease Editor form in Classic
*/

-- Gather 100 records in tblrecord that Ryan mapped and 100 of the records you're mapping
-- Set a value to index the records so you can sort in groups

-- Investigate 
SELECT TOP 100 tr.*
FROM countyScansTitle.dbo.tblrecord tr 
JOIN countyScansTitle.dbo.tblexportLog tel ON tel.recordID = tr.recordID
WHERE tel.zipName LIKE '%LEGACY%'
ORDER BY tel.LeaseID;

SELECT TOP 100 tr.*
FROM countyScansTitle.dbo.tblrecord tr
JOIN countyScansTitle.dbo.tblexportLog tel ON tel.recordID = tr.recordID
WHERE tel.zipName LIKE '%LND-6732%'
ORDER BY tel.LeaseID;


-- Data Integrity Checks
-- Check for records with null or empty essential fields that should have been populated
SELECT COUNT(*) AS records_with_null_essentials
FROM countyScansTitle.dbo.tblrecord
WHERE remarks LIKE '%LND-6732%'
  AND (recordNumber IS NULL 
    OR statusID != 90 
	OR recordIsLease != 1
	OR countyID IS NULL);

-- Verify that county IDs are valid in the migrated data
SELECT tr.recordID, tr.countyID 
FROM countyScansTitle.dbo.tblrecord tr
LEFT JOIN countyScansTitle.Tracker.MasterCountyLookup mcl ON mcl.LeasingID = tr.countyID
WHERE tr.remarks LIKE '%LND-6732%'
  AND tr.countyID IS NULL;

-- Check for records with future dates (possible data entry errors)
SELECT COUNT(*) AS future_dated_records
FROM countyScansTitle.dbo.tblrecord
WHERE remarks LIKE '%LND-6732%'
  AND instrumentDate > GETDATE();

-- Relationship Verification
-- Check if all migrated records have appropriate related records in tblexportLog
SELECT COUNT(*) AS records_without_export_log
FROM countyScansTitle.dbo.tblrecord tr
WHERE tr.remarks LIKE '%LND-6732%'
  AND NOT EXISTS (
    SELECT 1 FROM countyScansTitle.dbo.tblexportLog tel 
    WHERE tel.recordID = tr.recordID
  );

-- Expanded query to check if lease records have necessary relationships across all related tables
SELECT 
    'Records missing land descriptions' AS missing_relationship_type,
    COUNT(*) AS record_count
FROM 
    countyScansTitle.dbo.tblrecord tr
WHERE 
    tr.remarks LIKE '%LND-6732%'
    AND tr.recordIsLease = 1
    AND NOT EXISTS (
        SELECT 1 
        FROM countyScansTitle.dbo.tbllandDescription tld
        WHERE tld.recordID = tr.recordID
		-- Not All records contain a land description
    )

UNION ALL

-- Investigate the 4 missing Volume Page
SELECT 
    'Records missing grantor/grantee entries' AS missing_relationship_type,
    COUNT(*) AS record_count
FROM 
    countyScansTitle.dbo.tblrecord tr
WHERE 
    tr.remarks LIKE '%LND-6732%'
    AND tr.recordIsLease = 1
    AND NOT EXISTS (
        SELECT 1 
        FROM countyScansTitle.dbo.tblgrantorGrantee tgg
        WHERE tgg.recordID = tr.recordID
    )

UNION ALL

SELECT 
    'Records missing volume/page references' AS missing_relationship_type,
    COUNT(*) AS record_count
FROM 
    countyScansTitle.dbo.tblrecord tr
WHERE 
    tr.remarks LIKE '%LND-6732%'
    AND tr.recordIsLease = 1
    AND NOT EXISTS (
        SELECT 1 
        FROM countyScansTitle.dbo.tbldeedReferenceVolumePage tdr
        WHERE tdr.recordID = tr.recordID
		-- Not all records contain a Volume Page
    );

-- Source Target Comparison
-- Compare lease record counts by county between source and target
SELECT 
    mcl.County AS CountyName,
    mcl.State AS StateName,
    COUNT(DISTINCT ll.leaseID) AS SourceLeaseCount,
    COUNT(DISTINCT tel.leaseID) AS TargetLeaseCount,
    COUNT(DISTINCT ll.leaseID) - COUNT(DISTINCT tel.leaseID) AS Difference
FROM 
    [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] ll
LEFT JOIN 
    countyScansTitle.dbo.tblrecord tr ON tr.remarks LIKE '%LND-6732%'
LEFT JOIN 
    countyScansTitle.dbo.tblexportLog tel ON tr.recordID = tel.recordID
LEFT JOIN 
    countyScansTitle.Tracker.MasterCountyLookup mcl ON ll.CountyID = mcl.leasingID
GROUP BY 
    mcl.County, mcl.State
HAVING 
    COUNT(DISTINCT ll.leaseID) <> COUNT(DISTINCT tel.leaseID)
ORDER BY 
    Difference DESC;

SELECT TOP 1 *
FROM countyScansTitle.Tracker.MasterCountyLookup



-- Verify New Requirements Are Met
-- Check if all migrated records have recordIsLease = 1 & recordIsCourthouse = 0 
-- as mentioned in your notes
SELECT COUNT(*) AS incorrectly_configured_records
FROM countyScansTitle.dbo.tblrecord
WHERE remarks LIKE '%LND-6732%'
  AND (recordIsLease <> 1 OR recordIsCourthouse <> 0);

-- Verify all required instrument types were migrated
SELECT 
    InstrumentTypeID, 
    COUNT(*) AS RecordCount
FROM 
    countyScansTitle.dbo.tblrecord
WHERE 
    remarks LIKE '%LND-6732%'
GROUP BY 
    InstrumentTypeID
ORDER BY 
    RecordCount DESC;

-- Check status distribution of migrated records
SELECT 
    statusID, 
    COUNT(*) AS RecordCount
FROM 
    countyScansTitle.dbo.tblrecord
WHERE 
    remarks LIKE '%LND-6732%'
GROUP BY 
    statusID
ORDER BY 
    RecordCount DESC;
