USE countyScansTitle;
SET ANSI_NULLS ON;
SET ANSI_WARNINGS ON;
GO
/****************************************************************************************************************************************************************
** File:            LND-6732_Map DIV1 Lease Images To DIML.sql
** Author:          Donald Massey
** Copyright:       Enverus
** Creation Date:   2025-05-21
** Description:     Update tblS3Image With Imported DIV1 Lease Images To DIML
***************************************************************************************************************************************************************
** Date:        Author:             Description:
** --------     --------            -------------------------------------------
** 2025-05-21   Donald Massey       Update tblS3Image With Imported DIV1 Lease Images To DIML
***************************************************************************************************************************************************************/

-- Create the query to update the tblS3Image with the imported DIV1 Lease Images to DIML
SET XACT_ABORT ON;
BEGIN TRAN;

-- Query to update tbllandDescription using LND_6732_tblS3Image_20250717
INSERT INTO dbo.tblS3Image (
    recordID
   ,s3FilePath
   ,pageCount
   ,fileSizeBytes
   ,_ModifiedDateTime
   ,_ModifiedBy
)
SELECT
    tel.recordID AS recordID
   ,CASE
        WHEN LEFT(dest.destination_path, 5) = 's3://' THEN dest.destination_path
        ELSE CONCAT('s3://', dest.destination_path)
    END AS s3FilePath
   ,dest.page_count AS pageCount
   ,dest.file_size AS fileSizeBytes
   ,GETDATE() AS _ModifiedDateTime
   ,'LND-6732(2)' AS _ModifiedBy
SELECT COUNT(*)
FROM countyScansTitle.dbo.LND_6732_DEST_20250717 dest
JOIN countyScansTitle.dbo.tblexportLog tel ON tel.LeaseID = dest.leaseid
WHERE tel.zipName = 'LND-6732(2)'


-- Rollback the transaction (for testing purposes)
ROLLBACK TRAN;
-- Uncomment the following line to commit the transaction
--COMMIT TRAN;

-- Check transaction count and state
SELECT @@TRANCOUNT, XACT_STATE();



-- These tables are used by LND-6732_query_s3.py, which updates this table with the S3 locations for the Lease images from DIV1
--IF OBJECT_ID(N'countyScansTitle.dbo.LND_6732_SRC_20250717') IS NOT NULL
--  DROP TABLE countyScansTitle.dbo.LND_6732_SRC_20250717;

-- Count 2,081,043
SELECT tel.leaseid
, tel.recordid
, CONCAT(LOWER(tls.StateAbbreviation), '/', LOWER(tlc.CountyName)) AS state_countyname
, tlicr.package_id
, '                                                                                                                       ' AS source_path
, '                                                                                                                  ' AS destination_path
, 0                                                                                                                          AS page_count
, 0                                                                                                                           AS file_size
, '                                                                                                                           '  AS status
INTO countyScansTitle.dbo.LND_6732_SRC_20250717
FROM countyScansTitle.dbo.tblrecord tr
LEFT JOIN countyScansTitle.dbo.tblexportLog tel ON tr.recordID = tel.recordID
LEFT JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] tll ON tll.leaseID = tel.leaseID
LEFT JOIN CS_Digital.dbo.tblLeaseIDxref tlicr ON tlicr.lease_id = tel.LeaseID
LEFT JOIN countyScansTitle.dbo.tbllookupCounties tlc ON tr.countyID = tlc.CountyID
LEFT JOIN countyScansTitle.dbo.tbllookupStates tls ON tr.stateID = tls.StateID
LEFT JOIN countyScansTitle.dbo.tblS3Image tsi ON tsi.recordID = tel.recordID
WHERE package_id IS NOT NULL AND tsi.recordID IS NULL
ORDER BY tel.leaseid

SELECT COUNT(*)
FROM countyScansTitle.dbo.LND_6732_SRC_20250717
WHERE recordID IN (SELECT recordID FROM countyScansTitle.dbo.tblS3Image)
ORDER BY leaseID

SELECT recordID
FROM countyScansTitle.dbo.tblS3Image
WHERE recordID = 'd489a3ba-2d55-44f3-80b3-dd29a3447f42'


--IF OBJECT_ID(N'countyScansTitle.dbo.LND_6732_DEST_20250717') IS NOT NULL
--  DROP TABLE countyScansTitle.dbo.LND_6732_DEST_20250717;

CREATE TABLE [countyScansTitle].[dbo].[LND_6732_DEST_20250717](
	[leaseid] [int] NULL,
	[recordid] [varchar](36) NULL,
	[state_countyname] [nvarchar](266) NOT NULL,
	[package_id] [varchar](20) NULL,
	[source_path] [varchar](119) NOT NULL,
	[destination_path] [varchar](114) NOT NULL,
	[page_count] [int] NOT NULL,
	[file_size] [int] NOT NULL,
	[status] [varchar](123) NOT NULL)

CREATE UNIQUE CLUSTERED INDEX IX_LND_6732_DEST_20250717_leaseid
ON [countyScansTitle].[dbo].[LND_6732_DEST_20250717] (leaseid);

--IF OBJECT_ID(N'countyScansTitle.dbo.LND_6732_20250717') IS NOT NULL
--  DROP TABLE countyScansTitle.dbo.LND_6732_20250717;

CREATE TABLE [countyScansTitle].[dbo].[LND_6732_tblS3Image_20250717](
	[recordID] [varchar](36) NOT NULL,
	[s3FilePath] [varchar](300) NOT NULL,
	[pageCount] [int] NULL,
	[fileSizeBytes] [bigint] NULL,
	[_ModifiedDateTime] [datetime] NULL,
	[_ModifiedBy] [varchar](75) NULL,
	[status] [varchar](75) NULL)

-- Don't include these in the tblS3Image update statement
SELECT *
FROM [countyScansTitle].[dbo].[LND_6732_tblS3Image_20250717] s
JOIN [countyScansTitle].[dbo].[LND_6732_DEST_20250717] d ON s.recordID = d.recordid
WHERE pageCount = 0

SELECT COUNT(*) FROM countyScansTitle.dbo.LND_6732_DEST_20250717 WHERE status != 'processed';
SELECT * FROM countyScansTitle.dbo.LND_6732_SRC_20250717
SELECT * FROM countyScansTitle.dbo.LND_6732_DEST_20250717
-- Used to reduce the table until the process was complete, this was due to issues with the expiring tokens
/*
DELETE FROM countyScansTitle.dbo.LND_6732_DEST_20250717
WHERE status != 'processed';
*/

/*
DELETE FROM countyScansTitle.dbo.LND_6732_SRC_20250717
WHERE EXISTS (
    SELECT 1
    FROM countyScansTitle.dbo.LND_6732_DEST_20250717
    WHERE countyScansTitle.dbo.LND_6732_SRC_20250717.recordID = countyScansTitle.dbo.LND_6732_DEST_20250717.recordID
);
*/


-- Insert records from DestinationTable Back To SourceTable So The LND-6732_copy_images.py script can copy the files to the CHEA S3 bucket
INSERT INTO countyScansTitle.dbo.LND_6732_SRC_20250717 (leaseid, recordid, state_countyname, package_id, source_path, destination_path, page_count, file_size, status)
SELECT leaseid, recordid, state_countyname, package_id, source_path, destination_path, page_count, file_size, ''
FROM countyScansTitle.dbo.LND_6732_DEST_20250717
WHERE status != 'processed'


-- Remove Duplicates there's 15 
SELECT leaseID
FROM [countyScansTitle].[dbo].[LND_6732_DEST_20250717]
GROUP BY leaseID
HAVING COUNT(*) > 2


-- Verify records exist, Woo

SELECT *
FROM countyScansTitle.dbo.LND_6732_DEST_20250717
WHERE recordid = 'b440da2c-219f-419c-b069-b1144e732e0c'

SELECT TOP 100 *
FROM countyScansTitle.dbo.tblS3Image
WHERE recordid = 'b440da2c-219f-419c-b069-b1144e732e0c'

-- Promote Test Record To S3Image
SELECT *
FROM countyScansTitle.dbo.tblrecord tr
JOIN countyScansTitle.dbo.tblexportlog tel ON tel.recordid = tr.recordid
WHERE tr.recordid = 'b440da2c-219f-419c-b069-b1144e732e0c'



SET XACT_ABORT ON;
BEGIN TRAN;

INSERT INTO [countyScansTitle].[dbo].[tblS3Image] (recordID, s3FilePath, pageCount, fileSizeBytes, _ModifiedDateTime, _ModifiedBy)
VALUES ('b440da2c-219f-419c-b069-b1144e732e0c', 'enverus-courthouse-prod-chd-plants/ks/harper/b440/b440da2c-219f-419c-b069-b1144e732e0c.pdf', 2, 1269138, GETDATE(), 'LND-6732');

SELECT *
FROM countyScansTitle.dbo.tblS3Image
WHERE recordID = 'b440da2c-219f-419c-b069-b1144e732e0c'

-- Rollback the transaction (for testing purposes)
--ROLLBACK TRAN;
-- Uncomment the following line to commit the transaction
COMMIT TRAN;

-- Check transaction count and state
SELECT @@TRANCOUNT, XACT_STATE();


SELECT TOP 10 *
FROM countyScansTitle.dbo.tblS3Image



WITH RankedPages AS (
    SELECT 
        *,  -- selects all columns from the source table
        ROW_NUMBER() OVER (PARTITION BY page_count ORDER BY [page_count]) AS rn
    FROM 
        [countyScansTitle].[dbo].[LND_6732_DEST_20250717]
    WHERE 
        page_count IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
)
SELECT tr.recordNumber, *
FROM RankedPages rp
JOIN countyScansTitle.dbo.tblrecord tr ON tr.recordID = rp.recordid
WHERE rn <= 5
ORDER BY page_count, rn;

SELECT TOP 10 *
FROM countyScansTitle.dbo.tblS3Image
WHERE _ModifiedBy != 'cs_updates'
AND _ModifiedBy != 's3_upload_objects_backfill'
AND _ModifiedBy != 's3_list_objects_backfill'
AND _ModifiedBy != 'Tyler.Jordan'
AND _ModifiedBy != 'TylerJordan'
AND _ModifiedBy != 'NA\chad.hutcherson'