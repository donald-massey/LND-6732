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


SELECT 
    CountyID
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
    AND ll.updated BETWEEN '2006-12-14' AND '2021-09-29'
GROUP BY CountyID;

SELECT *
FROM countyScansTitle.Tracker.MasterCountyLookup
WHERE leasingID IN (SELECT 
    CountyID
	,COUNT(*)
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
    AND ll.updated BETWEEN '2006-12-14' AND '2021-09-29'
GROUP BY CountyID)

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
