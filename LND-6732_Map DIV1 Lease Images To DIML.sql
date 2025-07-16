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

-- Query to update tbllandDescription using LND_6732_tblS3Image_20250506
INSERT INTO dbo.tblS3Image (
    recordID
   ,s3FilePath
   ,pageCount
   ,fileSizeBytes
   ,_ModifiedDateTime
   ,_ModifiedBy
)
SELECT 
    recordID
   ,s3FilePath
   ,pageCount
   ,fileSizeBytes
   ,_ModifiedDateTime
   ,_ModifiedBy
FROM countyScansTitle.dbo.LND_6732_tblS3Image_20250506;


-- Rollback the transaction (for testing purposes)
ROLLBACK TRAN;
-- Uncomment the following line to commit the transaction
--COMMIT TRAN;

-- Check transaction count and state
SELECT @@TRANCOUNT, XACT_STATE();



-- These tables are used by LND-6732_query_s3.py, which updates this table with the S3 locations for the Lease images from DIV1
-- IF OBJECT_ID(N'countyScansTitle.dbo.LND_6732_SRC_20250506') IS NOT NULL
--	DROP TABLE countyScansTitle.dbo.LND_6732_SRC_20250506;

-- Count 2,081,043
SELECT tel.leaseid, tel.recordid, CONCAT(LOWER(tls.StateAbbreviation), '/', LOWER(tlc.CountyName)) AS state_countyname, tlicr.package_id,
'                                                                                                                       ' AS source_path,
'                                                                                                                  ' AS destination_path,
0                                                                                                                          AS page_count,
0                                                                                                                           AS file_size,
'                                                                                                                           '  AS status
INTO countyScansTitle.dbo.LND_6732_SRC_20250506
FROM countyScansTitle.dbo.tblrecord tr
LEFT JOIN countyScansTitle.dbo.tblexportLog tel ON tr.recordID = tel.recordID
LEFT JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] tll ON tll.leaseID = tel.leaseID
LEFT JOIN CS_Digital.dbo.tblLeaseIDxref tlicr ON tlicr.lease_id = tel.LeaseID
LEFT JOIN countyScansTitle.dbo.tbllookupCounties tlc ON tr.countyID = tlc.CountyID
LEFT JOIN countyScansTitle.dbo.tbllookupStates tls ON tr.stateID = tls.StateID
WHERE CONVERT(varchar, tr.receivedDate, 23) = '2025-05-06'
AND package_id IS NOT NULL
ORDER BY tel.leaseid


--IF OBJECT_ID(N'countyScansTitle.dbo.LND_6732_DEST_20250506') IS NOT NULL
--	DROP TABLE countyScansTitle.dbo.LND_6732_DEST_20250506;

CREATE TABLE [countyScansTitle].[dbo].[LND_6732_DEST_20250506](
	[leaseid] [int] NULL,
	[recordid] [varchar](36) NULL,
	[state_countyname] [nvarchar](266) NOT NULL,
	[package_id] [varchar](20) NULL,
	[source_path] [varchar](119) NOT NULL,
	[destination_path] [varchar](114) NOT NULL,
	[page_count] [int] NOT NULL,
	[file_size] [int] NOT NULL,
	[status] [varchar](123) NOT NULL)


--IF OBJECT_ID(N'countyScansTitle.dbo.LND_6732_20250506') IS NOT NULL
--	DROP TABLE countyScansTitle.dbo.LND_6732_20250506;

CREATE TABLE [countyScansTitle].[dbo].[LND_6732_20250506](
	[recordID] [varchar](36) NOT NULL,
	[s3FilePath] [varchar](300) NOT NULL,
	[pageCount] [int] NULL,
	[fileSizeBytes] [bigint] NULL,
	[_ModifiedDateTime] [datetime] NULL,
	[_ModifiedBy] [varchar](75) NULL,
	[status] [varchar](75) NULL)


-- Used to reduce the table until the process was complete, this was due to issues with the expiring tokens
/*
DELETE FROM countyScansTitle.dbo.LND_6732_SRC_20250506
WHERE EXISTS (
    SELECT 1
    FROM countyScansTitle.dbo.LND_6732_DEST_20250506
    WHERE countyScansTitle.dbo.LND_6732_SRC_20250506.recordID = countyScansTitle.dbo.LND_6732_DEST_20250506.recordID
);
*/

/*
DELETE FROM countyScansTitle.dbo.LND_6732_DEST_20250506
WHERE status != 'processed';
*/

/*
DELETE FROM countyScansTitle.dbo.LND_6732_SRC_20250506
WHERE leaseid in (SELECT leaseid from countyScansTitle.dbo.LND_6732_DEST_20250506)
*/

-- Insert records from DestinationTable Back To SourceTable So The LND-6732_copy_images.py script can copy the files to the CHEA S3 bucket
INSERT INTO countyScansTitle.dbo.LND_6732_SRC_20250506 (leaseid, recordid, state_countyname, package_id, source_path, destination_path, page_count, file_size, status)
SELECT leaseid, recordid, state_countyname, package_id, source_path, destination_path, page_count, file_size, ''
FROM countyScansTitle.dbo.LND_6732_DEST_20250506
WHERE status != 'processed'


-- QA Section
