USE countyScansTitle;
SET ANSI_NULLS ON;
SET ANSI_WARNINGS ON;
GO
/****************************************************************************************************************************************************************
** File:            LND-6838_Map Land Descriptions Values From DIV1 To CountyScansTitle.sql
** Author:          Donald Massey (Updated)
** Copyright:       Enverus
** Creation Date:   2025-05-21
** Description:     Update tbllandDescription With Imported DIV1 Lease Data
***************************************************************************************************************************************************************
** Date:        Author:             Description:
** --------     --------            -------------------------------------------
** 2025-05-21   Donald Massey       Update tbllandDescription With Imported DIV1 Lease Data
***************************************************************************************************************************************************************/


-- Create a query to check 10 records from each to verify this is working correctly
SET XACT_ABORT ON;
BEGIN TRAN;

-- Query to update tbllandDescription using LND_6838_tbllandDescription
UPDATE dbo.LND_6838_tbllandDescription
SET 
	 LND_6838_tbllandDescription.landDescriptionID = ld_20250519.landDescriptionID
	,LND_6838_tbllandDescription.recordID = ld_20250519.recordID
	,LND_6838_tbllandDescription.CountyID = ld_20250519.CountyID
	,LND_6838_tbllandDescription.Subdivision = ld_20250519.Subdivision
	,LND_6838_tbllandDescription.Survey = ld_20250519.Survey
	,LND_6838_tbllandDescription.Lot = ld_20250519.Lot
	,LND_6838_tbllandDescription.Block = ld_20250519.Block
	,LND_6838_tbllandDescription.Section = ld_20250519.Section
	,LND_6838_tbllandDescription.Township = ld_20250519.Township
	,LND_6838_tbllandDescription.RangeOrBlock = ld_20250519.RangeOrBlock
	,LND_6838_tbllandDescription.AbstractName = ld_20250519.AbstractName
	,LND_6838_tbllandDescription.Suffix = ld_20250519.Suffix
	,LND_6838_tbllandDescription.QuarterCalls = ld_20250519.QuarterCalls
	,LND_6838_tbllandDescription.AcreageByTract = ld_20250519.AcreageByTract
	,LND_6838_tbllandDescription.TractDRRecordNumber = ld_20250519.TractDRRecordNumber
	,LND_6838_tbllandDescription.TractDRVolumePage = ld_20250519.TractDRVolumePage
	,LND_6838_tbllandDescription.PlatVolumePage = ld_20250519.PlatVolumePage
	,LND_6838_tbllandDescription.PlatRecordNumber = ld_20250519.PlatRecordNumber
	,LND_6838_tbllandDescription.BriefLegal = ld_20250519.BriefLegal
	,LND_6838_tbllandDescription.EntryDate = ld_20250519.EntryDate
	,LND_6838_tbllandDescription.oldLandID = ld_20250519.oldLandID
	,LND_6838_tbllandDescription._CreatedDateTime = ld_20250519._CreatedDateTime
	,LND_6838_tbllandDescription._CreatedBy = ld_20250519._CreatedBy
	,LND_6838_tbllandDescription._ModifiedDateTime = ld_20250519._ModifiedDateTime
	,LND_6838_tbllandDescription._ModifiedBy = ld_20250519._ModifiedBy
	,LND_6838_tbllandDescription.AutoGenDate = ld_20250519.AutoGenDate
	,LND_6838_tbllandDescription.PatentNumber = ld_20250519.PatentNumber
	,LND_6838_tbllandDescription.PatentVolume = ld_20250519.PatentVolume
	,LND_6838_tbllandDescription.CertificateNumber = ld_20250519.CertificateNumber
	,LND_6838_tbllandDescription.FileNumber = ld_20250519.FileNumber
	,LND_6838_tbllandDescription.PlatVolumeCabinet = ld_20250519.PlatVolumeCabinet
	,LND_6838_tbllandDescription.PlatPageSlide = ld_20250519.PlatPageSlide
	,LND_6838_tbllandDescription.PlatBookType = ld_20250519.PlatBookType
	,LND_6838_tbllandDescription.QuarterCallsFull = ld_20250519.QuarterCallsFull
	,LND_6838_tbllandDescription.NewCityBlock = ld_20250519.NewCityBlock
	,LND_6838_tbllandDescription.SubdivisionNameId = ld_20250519.SubdivisionNameId
	,LND_6838_tbllandDescription.IsDeleted = ld_20250519.IsDeleted
	,LND_6838_tbllandDescription.CountyName = ld_20250519.CountyName
	,LND_6838_tbllandDescription.StateAbbreviation = ld_20250519.StateAbbreviation
FROM LND_6838_tblLandDescription_20250519 AS ld_20250519
WHERE LND_6838_tbllandDescription.landDescriptionID = ld_20250519.landDescriptionID;

-- Select given values from both tables to verify the update is correct


-- Rollback the transaction (for testing purposes)
ROLLBACK TRAN;
-- Uncomment the following line to commit the transaction
--COMMIT TRAN;

-- Check transaction count and state
SELECT @@TRANCOUNT, XACT_STATE();



IF OBJECT_ID(N'countyScansTitle.dbo.LND_6838_tbllandDescription') IS NOT NULL
	DROP TABLE countyScansTitle.dbo.LND_6838_tbllandDescription;

-- Base table to generate a landDescriptionID and then use to join back the values parsed by the brief-legals-parser
DECLARE @currentDateTime DATETIME = GETDATE();
SELECT LOWER(NEWID()) AS landDescriptionID
	  ,LOWER(tr.recordID) AS recordID  -- From tblRecord
	  ,tr.countyID AS CountyID  -- From tblRecord
	  ,CAST('' AS CHAR(300)) AS Subdivision  -- Parsed From BriefLegals
	  ,CAST('' AS CHAR(300)) AS Survey  -- Parsed From BriefLegals
	  ,CAST('' AS CHAR(100)) AS Lot  -- Parsed From BriefLegals
	  ,CAST('' AS CHAR(300)) AS Block  -- Parsed From BriefLegals
	  ,CAST('' AS CHAR(50)) AS Section  -- Parsed From BriefLegals
	  ,CAST('' AS CHAR(50)) AS Township  -- Parsed From BriefLegals
	  -- Investigate this before hand off to Christian M.
	  ,COALESCE(CONCAT(CAST(ta.Range AS INT), ta.RangeDirection), ta.SurveyBlock) AS RangeOrBlock -- ta.Range, 
	  ,ta.AbstractNo AS AbstractName  -- From tblAbstract
	  ,CAST('' AS CHAR(50)) AS Suffix  -- Default Value
	  ,CAST('' AS CHAR(500)) AS QuarterCalls  -- Parsed From BriefLegals
	  ,CAST('' AS CHAR(50)) AS AcreageByTract  -- No Values
	  ,CAST('' AS CHAR(300)) AS TractDRRecordNumber  -- No values
	  ,CAST('' AS CHAR(300)) AS TractDRVolumePage  -- No values
	  ,CAST('' AS CHAR(300)) AS PlatVolumePage  -- No values
	  ,CAST('' AS CHAR(300)) AS PlatRecordNumber  -- No Values
	  ,tlam.descriptions AS BriefLegal -- From tblleaseAbstractMapping
	  ,@currentDateTime AS EntryDate -- Default Value
	  ,CAST('' AS INT) AS oldLandID -- No Values
	  ,@currentDateTime AS _CreatedDateTime -- Default Value
	  ,'LND-6838' AS _CreatedBy -- Default Value
	  ,@currentDateTime AS _ModifiedDateTime -- Default Value
	  ,'LND-6838' AS _ModifiedBy -- Default Value
	  ,@currentDateTime AS AutoGenDate -- Default Value
	  ,CAST('' AS CHAR(50)) AS PatentNumber -- Default Values
	  ,CAST('' AS CHAR(50)) AS PatentVolume  -- Default Values
	  ,CAST('' AS CHAR(50)) AS CertificateNumber  -- Default Values
	  ,CAST('' AS CHAR(50)) AS FileNumber  -- Default Values
	  ,CAST('' AS CHAR(10)) AS PlatVolumeCabinet  -- Default Values
	  ,CAST('' AS CHAR(10)) AS PlatPageSlide  -- Default Values
	  ,CAST('' AS CHAR(20)) AS PlatBookType  -- Default Values
	  ,CAST('' AS CHAR(500)) AS QuarterCallsFull  -- Parsed From BriefLegals
	  ,CAST('' AS CHAR(100)) AS NewCityBlock  -- Parsed From BriefLegals
	  ,CAST('' AS INT) AS SubdivisionNameId  -- Default Values
	  ,CAST(0 AS BIT) AS IsDeleted  -- Default Value
	  ,tlc.CountyName AS CountyName
	  ,tls.StateAbbreviation AS StateAbbreviation
INTO countyScansTitle.dbo.LND_6838_tbllandDescription
FROM [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] tll
LEFT JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLeaseDocumentMapping] tlldm ON tlldm.leaseid = tll.leaseid
LEFT JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblleaseAbstractMapping] tlam ON tlam.mappingid = tlldm.legalleasedocumentmappingid
LEFT JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblAbstract] ta ON ta.abstractid = tlam.abstractid
LEFT JOIN countyScansTitle.dbo.tblExportLog tel ON tll.leaseid = tel.LeaseID
LEFT JOIN countyScansTitle.dbo.tblrecord tr ON tel.recordID = tr.recordID
LEFT JOIN countyScansTitle.dbo.tbllookupStates tls ON tls.StateID = tr.stateID
LEFT JOIN countyScansTitle.dbo.tbllookupCounties tlc ON tlc.CountyID = tr.countyID
WHERE CAST(tr.receivedDate AS DATE) = '2025-05-06';


-- Gather records for brief-legals-parser -> CSV
/*
SELECT landDescriptionID, BriefLegal, StateAbbreviation, CountyName
FROM countyScansTitle.dbo.LND_6838_tbllandDescription
WHERE BriefLegal IS NOT NULL AND BriefLegal != ''
ORDER BY CountyName;
*/




-- QA Section <-- Standardize this
-- Create a better section for these QA queries moving forward
SELECT *
FROM countyScansTitle.dbo.LND_6838_temp_table

SELECT *
INTO countyScansTitle.dbo.LND_6838_tbllandDescription_backup
FROM countyScansTitle.dbo.LND_6838_tbllandDescription

IF OBJECT_ID(N'countyScansTitle.dbo.LND_6838_tbllandDescription_20250519') IS NOT NULL
	DROP TABLE countyScansTitle.dbo.LND_6838_tbllandDescription_20250519;

SELECT *
INTO countyScansTitle.dbo.LND_6838_tbllandDescription_20250519
FROM countyScansTitle.dbo.LND_6838_tbllandDescription
WHERE BriefLegal IS NOT NULL AND BriefLegal != ''


SELECT COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription_20250519

SELECT BriefLegal, *
FROM countyScansTitle.dbo.LND_6838_tbllandDescription_20250519
WHERE (BriefLegal LIKE ('%NE4%') OR BriefLegal LIKE ('%NW4%') 
    OR BriefLegal LIKE ('%SE4%') OR BriefLegal LIKE ('%SW4%')
	OR BriefLegal LIKE ('%S2%')  OR BriefLegal LIKE ('%N2%')
	OR BriefLegal LIKE ('%E2%')  OR BriefLegal LIKE ('%W2%')
	
	OR BriefLegal LIKE ('%NE/4%') OR BriefLegal LIKE ('%NW/4%') 
    OR BriefLegal LIKE ('%SE/4%') OR BriefLegal LIKE ('%SW/4%')
	OR BriefLegal LIKE ('%S/2%')  OR BriefLegal LIKE ('%N/2%')
	OR BriefLegal LIKE ('%E/2%')  OR BriefLegal LIKE ('%W/2%'))




SELECT BriefLegal, *
FROM countyScansTitle.dbo.LND_6838_tbllandDescription_20250519
WHERE recordID NOT IN (SELECT recordID FROM countyScansTitle.dbo.LND_6838_tbllandDescription_20250519 
WHERE (BriefLegal LIKE ('%NE4%') OR BriefLegal LIKE ('%NW4%') 
    OR BriefLegal LIKE ('%SE4%') OR BriefLegal LIKE ('%SW4%')
	OR BriefLegal LIKE ('%S2%')  OR BriefLegal LIKE ('%N2%')
	OR BriefLegal LIKE ('%E2%')  OR BriefLegal LIKE ('%W2%')
	
	OR BriefLegal LIKE ('%NE/4%') OR BriefLegal LIKE ('%NW/4%') 
    OR BriefLegal LIKE ('%SE/4%') OR BriefLegal LIKE ('%SW/4%')
	OR BriefLegal LIKE ('%S/2%')  OR BriefLegal LIKE ('%N/2%')
	OR BriefLegal LIKE ('%E/2%')  OR BriefLegal LIKE ('%W/2%')

	OR BriefLegal LIKE ('%SWNW%') OR BriefLegal LIKE ('%SWNE%')
	OR BriefLegal LIKE ('%SWSW%') OR BriefLegal LIKE ('%SWSE%') 
	OR BriefLegal LIKE ('%NWNW%') OR BriefLegal LIKE ('%NWNE%') 
	OR BriefLegal LIKE ('%NWSW%') OR BriefLegal LIKE ('%NWSE%')))


SELECT *
FROM countyScansTitle.dbo.LND_6838_temp_table
WHERE landDescriptionID IN (SELECT landDescriptionID FROM countyScansTitle.dbo.LND_6838_tbllandDescription_20250519)

ALTER TABLE countyScansTitle.dbo.LND_6838_tbllandDescription
ALTER COLUMN Lot char(100);

-- Need to investigate and modify records with Lot LEN() > 50

SELECT TOP 1 *
FROM countyScansTitle.dbo.LND_6838_tbllandDescription_20250519

-- QA Section
SELECT DISTINCT Subdivision, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription_20250519
GROUP BY Subdivision
ORDER BY COUNT(*) DESC

SELECT DISTINCT Survey, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription_20250519
GROUP BY Survey
ORDER BY COUNT(*) DESC

SELECT DISTINCT Lot, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription_20250519
GROUP BY Lot
ORDER BY COUNT(*) DESC

SELECT DISTINCT Block, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription_20250519
GROUP BY Block
ORDER BY COUNT(*) DESC

SELECT DISTINCT Section, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription_20250519
GROUP BY Section
ORDER BY COUNT(*) DESC

SELECT DISTINCT Township, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription_20250519
GROUP BY Township
ORDER BY COUNT(*) DESC

SELECT DISTINCT RangeOrBlock, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription_20250519
GROUP BY RangeorBlock
ORDER BY COUNT(*) DESC

SELECT DISTINCT AbstractName, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription_20250519
GROUP BY AbstractName
ORDER BY COUNT(*) DESC

SELECT DISTINCT Suffix, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription_20250519
GROUP BY Suffix
ORDER BY COUNT(*) DESC

SELECT DISTINCT QuarterCalls, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription_20250519
GROUP BY QuarterCalls
ORDER BY COUNT(*) DESC

SELECT DISTINCT AcreageByTract, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription_20250519
GROUP BY AcreageByTract
ORDER BY COUNT(*) DESC
