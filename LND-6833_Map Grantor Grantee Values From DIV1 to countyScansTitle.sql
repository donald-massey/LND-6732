USE countyScansTitle;
SET ANSI_NULLS ON;
SET ANSI_WARNINGS ON;
GO
/****************************************************************************************************************************************************************
** File:            LND-6833_Map Grantor Grantee Values From DIV1 to countyScansTitle.sql
** Author:          Donald Massey (Updated)
** Copyright:       Enverus
** Creation Date:   2025-04-02
** Description:     Map Grantor Grantee Values From DIV1 to countyScansTitle
***************************************************************************************************************************************************************
** Date:        Author:             Description:
** --------     --------            -------------------------------------------
** 2025-03-19   Donald Massey       Map Grantor Grantee Values From DIV1 to countyScansTitle
***************************************************************************************************************************************************************/

SET XACT_ABORT ON;
BEGIN TRAN;

-- Check that records not updated are correct

/*
IF OBJECT_ID('tempdb.dbo.#TempLegalLease', 'U') IS NOT NULL
    DROP TABLE tempdb.dbo.#TempLegalLease;

SELECT *
INTO #TempLegalLease
FROM [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease];

CREATE INDEX IX_TempLegalLease_LeaseID ON #TempLegalLease(LeaseID);

IF OBJECT_ID('tempdb.dbo.#TempLeaseGrantor', 'U') IS NOT NULL
    DROP TABLE tempdb.dbo.#TempLeaseGrantor

SELECT *
INTO #TempLeaseGrantor
FROM [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLeaseGrantor]
WHERE GrantorNAME != '';

CREATE INDEX IX_TempLeaseGrantor_GrantorID ON #TempLeaseGrantor(GrantorID);

IF OBJECT_ID('tempdb.dbo.#TempLeaseGrantee', 'U') IS NOT NULL
    DROP TABLE tempdb.dbo.#TempLeaseGrantee

SELECT *
INTO #TempLeaseGrantee
FROM [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLeaseGrantee]
WHERE GranteeNAME != '';

CREATE INDEX IX_TempLeaseGrantee_GranteeID ON #TempLeaseGrantee(GranteeID);


IF OBJECT_ID('countyScansTitle.dbo.LND_6833_tblgrantorGrantee', 'U') IS NOT NULL
    DROP TABLE countyScansTitle.dbo.LND_6833_tblgrantorGrantee;


-- Create a query to identify the records HAVING COUNT(*) > 2, that will identify the many:1 matches
-- Create a query to do a groupBY recordID to see if that seems off?
-- Create NULLIF for g* columns 
DECLARE @currentDateTime DATETIME = GETDATE();
-- 3,751,610 Records
WITH tblRecord AS (
	SELECT
		LOWER(tr.recordID) AS recordID,
		tel.LeaseID
	FROM countyScansTitle.dbo.tblrecord tr
	LEFT JOIN countyScansTitle.dbo.tblExportLog tel ON tel.recordID = tr.recordID
	WHERE CONVERT(varchar, tr.receivedDate, 23) = '2025-05-06'
),
-- 3,911,697 Records
GrantorData AS (
    SELECT 
        tr.recordID,
		tlg.GrantorID,
        'grantor' AS recordType,
        tlg.GrantorNAME AS gName,
        tlg.ADDRESS AS gAddress,
        tlg.ZIP AS gZip,
        tlg.CITY AS gCity,
        tls.StateID AS StateID,
        @currentDateTime AS _CreatedDateTime,
        'donald-massey' AS _CreatedBy,
        @currentDateTime AS _ModifiedDateTime,
        'donald-massey' AS _ModifiedBy,
        NULL AS interestedAssigned,
        0 AS IsDeleted,
        0 AS IsPrimary
    FROM #TempLeaseGrantor tlg
	INNER JOIN #TempLegalLease tll ON tlg.GrantorID = tll.GrantorID
	INNER JOIN tblRecord tr ON tll.LeaseID = tr.LeaseID
    LEFT JOIN countyScansTitle.dbo.tbllookupStates tls ON tlg.STATE = tls.StateAbbreviation
),
-- 3,810,407 Records
GranteeData AS (
    SELECT 
        tr.recordID,
        'grantee' AS recordType,
        tee.GranteeNAME AS gName,
        tee.ADDRESS AS gAddress,
        tee.ZIP AS gZip,
        tee.CITY AS gCity,
        tls.StateID AS StateID,
        @currentDateTime AS _CreatedDateTime,
        'donald-massey' AS _CreatedBy,
        @currentDateTime AS _ModifiedDateTime,
        'donald-massey' AS _ModifiedBy,
        NULL AS interestedAssigned,
        0 AS IsDeleted,
        0 AS IsPrimary
    FROM #TempLeaseGrantee tee
	INNER JOIN #TempLegalLease tll ON tee.GranteeID = tll.GranteeID
	INNER JOIN tblRecord tr ON tll.LeaseID = tr.LeaseID
    LEFT JOIN countyScansTitle.dbo.tbllookupStates tls ON tls.StateAbbreviation = tee.STATE
)
SELECT *
INTO countyScansTitle.dbo.LND_6833_tblgrantorGrantee
FROM GrantorData
UNION
SELECT * FROM GranteeData;
*/

-- Rollback the transaction (for testing purposes)
ROLLBACK TRAN;
-- Uncomment the following line to commit the transaction
--COMMIT TRAN;

-- Check transaction count and state
SELECT @@TRANCOUNT, XACT_STATE();


-- QA Section
-- tblRecord Total Records: 3,751,610

-- GrantorData Total Records: 3,911,697
-- Grantor Updates: 3,911,697

-- GranteeData Total Records: 3,810,407
-- Grantee Updates: 3,810,407


-- NULL Counts
SELECT 
    SUM(CASE WHEN recordID IS NULL THEN 1 ELSE 0 END) AS recordID_Null_Count,
    SUM(CASE WHEN recordID IS NOT NULL THEN 1 ELSE 0 END) AS recordID_NotNull_Count,
	SUM(CASE WHEN recordType IS NULL THEN 1 ELSE 0 END) AS recordType_Null_Count,
    SUM(CASE WHEN recordType IS NOT NULL THEN 1 ELSE 0 END) AS recordType_NotNull_Count,
    SUM(CASE WHEN gName IS NULL THEN 1 ELSE 0 END) AS gName_Null_Count,
    SUM(CASE WHEN gName IS NOT NULL THEN 1 ELSE 0 END) AS gName_NotNull_Count,
    SUM(CASE WHEN gAddress IS NULL THEN 1 ELSE 0 END) AS gAddress_Null_Count,
    SUM(CASE WHEN gAddress IS NOT NULL THEN 1 ELSE 0 END) AS gAddress_NotNull_Count,
    SUM(CASE WHEN gZip IS NULL THEN 1 ELSE 0 END) AS gZip_Null_Count,
    SUM(CASE WHEN gZip IS NOT NULL THEN 1 ELSE 0 END) AS gZip_NotNull_Count,
    SUM(CASE WHEN gCity IS NULL THEN 1 ELSE 0 END) AS gCity_Null_Count,
    SUM(CASE WHEN gCity IS NOT NULL THEN 1 ELSE 0 END) AS gCity_NotNull_Count,
    SUM(CASE WHEN StateID IS NULL THEN 1 ELSE 0 END) AS StateID_Null_Count,
    SUM(CASE WHEN StateID IS NOT NULL THEN 1 ELSE 0 END) AS StateID_NotNull_Count
FROM countyScansTitle.dbo.LND_6833_tblgrantorGrantee
WHERE recordType = 'grantor'


SELECT 
    SUM(CASE WHEN recordID IS NULL THEN 1 ELSE 0 END) AS recordID_Null_Count,
    SUM(CASE WHEN recordID IS NOT NULL THEN 1 ELSE 0 END) AS recordID_NotNull_Count,
	SUM(CASE WHEN recordType IS NULL THEN 1 ELSE 0 END) AS recordType_Null_Count,
    SUM(CASE WHEN recordType IS NOT NULL THEN 1 ELSE 0 END) AS recordType_NotNull_Count,
    SUM(CASE WHEN gName IS NULL THEN 1 ELSE 0 END) AS gName_Null_Count,
    SUM(CASE WHEN gName IS NOT NULL THEN 1 ELSE 0 END) AS gName_NotNull_Count,
    SUM(CASE WHEN gAddress IS NULL THEN 1 ELSE 0 END) AS gAddress_Null_Count,
    SUM(CASE WHEN gAddress IS NOT NULL THEN 1 ELSE 0 END) AS gAddress_NotNull_Count,
    SUM(CASE WHEN gZip IS NULL THEN 1 ELSE 0 END) AS gZip_Null_Count,
    SUM(CASE WHEN gZip IS NOT NULL THEN 1 ELSE 0 END) AS gZip_NotNull_Count,
    SUM(CASE WHEN gCity IS NULL THEN 1 ELSE 0 END) AS gCity_Null_Count,
    SUM(CASE WHEN gCity IS NOT NULL THEN 1 ELSE 0 END) AS gCity_NotNull_Count,
    SUM(CASE WHEN StateID IS NULL THEN 1 ELSE 0 END) AS StateID_Null_Count,
    SUM(CASE WHEN StateID IS NOT NULL THEN 1 ELSE 0 END) AS StateID_NotNull_Count
FROM countyScansTitle.dbo.LND_6833_tblgrantorGrantee
WHERE recordType = 'grantee';


SELECT *
FROM countyScansTitle.dbo.LND_6833_tblgrantorGrantee
WHERE recordid in (
SELECT recordID
FROM countyScansTitle.dbo.LND_6833_tblgrantorGrantee
WHERE recordType = 'grantor'
GROUP BY recordID
HAVING COUNT(*) > 1)
UNION
SELECT *
FROM countyScansTitle.dbo.LND_6833_tblgrantorGrantee
WHERE recordid in (
SELECT recordID
FROM countyScansTitle.dbo.LND_6833_tblgrantorGrantee
WHERE recordType = 'grantee'
GROUP BY recordID
HAVING COUNT(*) > 1)

SELECT TOP 1 *
FROM #TempLeaseGrantor
WHERE GrantorID = 2720619