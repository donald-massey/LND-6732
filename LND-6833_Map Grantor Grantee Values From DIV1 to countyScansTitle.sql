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

/*
IF OBJECT_ID('countyScansTitle.dbo.LND_6833_tblgrantorGrantee', 'U') IS NOT NULL
    DROP TABLE countyScansTitle.dbo.LND_6833_tblgrantorGrantee;

-- Optimized Query
WITH GrantorData AS (
    SELECT 
        LOWER(tr.recordID) AS recordID,
        'grantor' AS recordType,
        tor.GrantorNAME AS gName,
        tor.ADDRESS AS gAddress,
        tor.ZIP AS gZip,
        tor.CITY AS gCity,
        tls.StateID AS StateID,
        '2025-04-22 13:04:58' AS _CreatedDateTime,
        'donald-massey' AS _CreatedBy,
        '2025-04-22 13:04:58' AS _ModifiedDateTime,
        'donald-massey' AS _ModifiedBy,
        NULL AS interestedAssigned,
        0 AS IsDeleted,
        0 AS IsPrimary
    FROM countyScansTitle.dbo.tblrecord tr
    INNER JOIN countyScansTitle.dbo.tblExportLog tel ON tr.recordID = tel.recordID
    INNER JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] tll ON tel.LeaseID = tll.LeaseID
    INNER JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLeaseGrantor] tor ON tll.GrantorID = tor.GrantorID
    INNER JOIN countyScansTitle.dbo.tbllookupStates tls ON tls.StateAbbreviation = tor.STATE
    WHERE tor.GrantorNAME != '' AND tr.receivedDate = '2025-04-07'
),
GranteeData AS (
    SELECT 
        LOWER(tr.recordID) AS recordID,
        'grantee' AS recordType,
        tee.GranteeNAME AS gName,
        tee.ADDRESS AS gAddress,
        tee.ZIP AS gZip,
        tee.CITY AS gCity,
        tls.StateID AS StateID,
        '2025-04-22 13:04:58' AS _CreatedDateTime,
        'donald-massey' AS _CreatedBy,
        '2025-04-22 13:04:58' AS _ModifiedDateTime,
        'donald-massey' AS _ModifiedBy,
        NULL AS interestedAssigned,
        0 AS IsDeleted,
        0 AS IsPrimary
    FROM countyScansTitle.dbo.tblrecord tr
    INNER JOIN countyScansTitle.dbo.tblExportLog tel ON tr.recordID = tel.recordID
    INNER JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] tll ON tel.LeaseID = tll.LeaseID
    INNER JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLeaseGrantee] tee ON tll.GranteeID = tee.GranteeID
    INNER JOIN countyScansTitle.dbo.tbllookupStates tls ON tls.StateAbbreviation = tee.STATE
    WHERE tee.GranteeNAME != '' AND tr.receivedDate = '2025-04-07'
)
INSERT INTO countyScansTitle.dbo.LND_6833_tblgrantorGrantee
SELECT * FROM GrantorData
UNION ALL
SELECT * FROM GranteeData;
*/

-- Rollback the transaction (for testing purposes)
ROLLBACK TRAN;
-- Uncomment the following line to commit the transaction
--COMMIT TRAN;

-- Check transaction count and state
SELECT @@TRANCOUNT, XACT_STATE();

-- Gather the count of recordID and see if that's off
-- Gather a null count comparison of this same query to see if gName gAddress, gZip, gCity have excessive nulls


-- Get the total modified record Counts
-- 3,802,980 Records are updated in tblrecord
-- 4,969,884 Grantor Count
SELECT 
    COUNT(DISTINCT LOWER(tr.recordID)) AS unique_recordid_count, 'grantor_count' AS rec_count
FROM countyScansTitle.dbo.tblrecord tr
INNER JOIN countyScansTitle.dbo.tblExportLog tel ON tr.recordID = tel.recordID
INNER JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] tll ON tel.LeaseID = tll.LeaseID
INNER JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLeaseGrantor] tor ON tll.GrantorID = tor.GrantorID
WHERE GrantorNAME != '' AND CONVERT(varchar, tr.receivedDate, 23) = '2025-04-07'

UNION ALL

-- 4,969,920 Grantee Count
SELECT 
    COUNT(DISTINCT LOWER(tr.recordID)) AS unique_recordid_count, 'grantee_count' AS rec_count
FROM countyScansTitle.dbo.tblrecord tr
INNER JOIN countyScansTitle.dbo.tblExportLog tel ON tr.recordID = tel.recordID
INNER JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] tll ON tel.LeaseID = tll.LeaseID
INNER JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLeaseGrantee] tee ON tll.GranteeID = tee.GranteeID
WHERE GranteeNAME != '' AND CONVERT(varchar, tr.receivedDate, 23) = '2025-04-07';



-- NULL Count
SELECT 
    LOWER(tr.recordID) AS recordID,
    'grantor' AS recordType,
    GrantorNAME AS gName,
    tor.ADDRESS AS gAddress,
    tor.ZIP AS gZip,
    tor.CITY AS gCity,
    tr.StateID AS StateID,
    '2025-04-22 13:04:58' AS _CreatedDateTime,
    'donald-massey' AS _CreatedBy,
    '2025-04-22 13:04:58' AS _ModifiedDateTime,
    'donald-massey' AS _ModifiedBy,
    NULL AS interestedAssigned,
    0 AS IsDeleted,
    0 AS IsPrimary,
    COUNT(CASE WHEN GrantorNAME IS NULL THEN 1 ELSE NULL END) AS GrantorName_Null_Count,
    COUNT(CASE WHEN GrantorNAME IS NOT NULL THEN 1 ELSE NULL END) AS GrantorName_NotNull_Count,
    COUNT(CASE WHEN tor.ADDRESS IS NULL THEN 1 ELSE NULL END) AS Address_Null_Count,
    COUNT(CASE WHEN tor.ADDRESS IS NOT NULL THEN 1 ELSE NULL END) AS Address_NotNull_Count,
    COUNT(CASE WHEN tor.ZIP IS NULL THEN 1 ELSE NULL END) AS Zip_Null_Count,
    COUNT(CASE WHEN tor.ZIP IS NOT NULL THEN 1 ELSE NULL END) AS Zip_NotNull_Count,
    COUNT(CASE WHEN tor.CITY IS NULL THEN 1 ELSE NULL END) AS City_Null_Count,
    COUNT(CASE WHEN tor.CITY IS NOT NULL THEN 1 ELSE NULL END) AS City_NotNull_Count,
    COUNT(CASE WHEN tr.StateID IS NULL THEN 1 ELSE NULL END) AS StateID_Null_Count,
    COUNT(CASE WHEN tr.StateID IS NOT NULL THEN 1 ELSE NULL END) AS StateID_NotNull_Count
FROM countyScansTitle.dbo.tblrecord tr
INNER JOIN countyScansTitle.dbo.tblExportLog tel ON tr.recordID = tel.recordID
INNER JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] tll ON tel.LeaseID = tll.LeaseID
INNER JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLeaseGrantor] tor ON tll.GrantorID = tor.GrantorID
WHERE GrantorNAME != ''
GROUP BY 
    LOWER(tr.recordID), GrantorNAME, tor.ADDRESS, tor.ZIP, tor.CITY, tr.StateID

UNION ALL

SELECT 
    LOWER(tr.recordID) AS recordID,
    'grantee' AS recordType,
    GranteeNAME AS gName,
    tee.ADDRESS AS gAddress,
    tee.ZIP AS gZip,
    tee.CITY AS gCity,
    tr.StateID AS StateID,
    '2025-04-22 13:04:58' AS _CreatedDateTime, 
    'donald-massey' AS _CreatedBy,
    '2025-04-22 13:04:58' AS _ModifiedDateTime,
    'donald-massey' AS _ModifiedBy,
    NULL AS interestedAssigned,
    0 AS IsDeleted,
    0 AS IsPrimary,
    COUNT(CASE WHEN GranteeNAME IS NULL THEN 1 ELSE NULL END) AS GranteeName_Null_Count,
    COUNT(CASE WHEN GranteeNAME IS NOT NULL THEN 1 ELSE NULL END) AS GranteeName_NotNull_Count,
    COUNT(CASE WHEN tee.ADDRESS IS NULL THEN 1 ELSE NULL END) AS Address_Null_Count,
    COUNT(CASE WHEN tee.ADDRESS IS NOT NULL THEN 1 ELSE NULL END) AS Address_NotNull_Count,
    COUNT(CASE WHEN tee.ZIP IS NULL THEN 1 ELSE NULL END) AS Zip_Null_Count,
    COUNT(CASE WHEN tee.ZIP IS NOT NULL THEN 1 ELSE NULL END) AS Zip_NotNull_Count,
    COUNT(CASE WHEN tee.CITY IS NULL THEN 1 ELSE NULL END) AS City_Null_Count,
    COUNT(CASE WHEN tee.CITY IS NOT NULL THEN 1 ELSE NULL END) AS City_NotNull_Count,
    COUNT(CASE WHEN tr.StateID IS NULL THEN 1 ELSE NULL END) AS StateID_Null_Count,
    COUNT(CASE WHEN tr.StateID IS NOT NULL THEN 1 ELSE NULL END) AS StateID_NotNull_Count
FROM countyScansTitle.dbo.tblrecord tr
INNER JOIN countyScansTitle.dbo.tblExportLog tel ON tr.recordID = tel.recordID
INNER JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] tll ON tel.LeaseID = tll.LeaseID
INNER JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLeaseGrantee] tee ON tll.GranteeID = tee.GranteeID
WHERE GranteeNAME != ''
GROUP BY 
    LOWER(tr.recordID), GranteeNAME, tee.ADDRESS, tee.ZIP, tee.CITY, tr.StateID