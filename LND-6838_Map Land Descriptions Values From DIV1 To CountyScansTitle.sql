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
INSERT INTO dbo.tbllandDescription (
    landDescriptionID,
    recordID,
    CountyID,
    Subdivision,
    Survey,
    Lot,
    Block,
    Section,
    Township,
    RangeOrBlock,
    AbstractName,
    Suffix,
    QuarterCalls,
    AcreageByTract,
    TractDRRecordNumber,
    TractDRVolumePage,
    PlatVolumePage,
    PlatRecordNumber,
    BriefLegal,
    EntryDate,
    oldLandID,
    _CreatedDateTime,
    _CreatedBy,
    _ModifiedDateTime,
    _ModifiedBy,
    AutoGenDate,
    PatentNumber,
    PatentVolume,
    CertificateNumber,
    FileNumber,
    PlatVolumeCabinet,
    PlatPageSlide,
    PlatBookType,
    QuarterCallsFull,
    NewCityBlock,
    SubdivisionNameId,
    IsDeleted
)
SELECT 
    LOWER(NEWID()),
    recordID,
    CountyID,
    Subdivision,
    Survey,
    Lot,
    Block,
    Section,
    Township,
    RangeOrBlock,
    AbstractName,
    Suffix,
    QuarterCalls,
    AcreageByTract,
    TractDRRecordNumber,
    TractDRVolumePage,
    PlatVolumePage,
    PlatRecordNumber,
    BriefLegal,
    EntryDate,
    oldLandID,
    _CreatedDateTime,
    _CreatedBy,
    _ModifiedDateTime,
    _ModifiedBy,
    AutoGenDate,
    PatentNumber,
    PatentVolume,
    CertificateNumber,
    FileNumber,
    PlatVolumeCabinet,
    PlatPageSlide,
    PlatBookType,
    QuarterCallsFull,
    NewCityBlock,
    SubdivisionNameId,
    IsDeleted
FROM countyScansTitle.dbo.LND_6838_tblLandDescription AS LND_6838_tld;


-- Rollback the transaction (for testing purposes)
--ROLLBACK TRAN;
-- Uncomment the following line to commit the transaction
COMMIT TRAN;

-- Check transaction count and state
SELECT @@TRANCOUNT, XACT_STATE();



IF OBJECT_ID(N'countyScansTitle.dbo.LND_6838_tbllandDescription') IS NOT NULL
	DROP TABLE countyScansTitle.dbo.LND_6838_tbllandDescription;

-- Base table to generate a landDescriptionID and then use to join back the values parsed by the brief-legals-parser
DECLARE @currentDateTime DATETIME = GETDATE();
SELECT DISTINCT LOWER(NEWID()) AS landDescriptionID
	  ,LOWER(tr.recordID) AS recordID  -- From tblRecord
	  ,tr.countyID AS CountyID  -- From tblRecord
	  ,NULLIF(CAST('' AS CHAR(300)),'') AS Subdivision  -- Parsed From BriefLegals
	  ,NULLIF(CAST('' AS CHAR(300)),'') AS Survey  -- Parsed From BriefLegals
	  ,NULLIF(CAST('' AS CHAR(100)),'') AS Lot  -- Parsed From BriefLegals
	  ,NULLIF(CAST('' AS CHAR(300)),'') AS Block  -- Parsed From BriefLegals
	  ,NULLIF(CAST('' AS CHAR(50)),'') AS Section  -- Parsed From BriefLegals
	  ,NULLIF(CAST('' AS CHAR(50)),'') AS Township  -- Parsed From BriefLegals
	  -- Investigate this before hand off to Christian M.
	  ,NULLIF(COALESCE(CONCAT(CAST(ta.Range AS INT), ta.RangeDirection), ta.SurveyBlock),'') AS RangeOrBlock -- ta.Range, 
	  ,ta.AbstractNo AS AbstractName  -- From tblAbstract
	  ,NULLIF(CAST('' AS CHAR(50)),'') AS Suffix  -- Default Value
	  ,NULLIF(CAST('' AS CHAR(500)),'') AS QuarterCalls  -- Parsed From BriefLegals
	  ,NULLIF(CAST('' AS CHAR(50)),'') AS AcreageByTract  -- No Values
	  ,NULLIF(CAST('' AS CHAR(300)),'') AS TractDRRecordNumber  -- No values
	  ,NULLIF(CAST('' AS CHAR(300)),'') AS TractDRVolumePage  -- No values
	  ,NULLIF(CAST('' AS CHAR(300)),'') AS PlatVolumePage  -- No values
	  ,NULLIF(CAST('' AS CHAR(300)),'') AS PlatRecordNumber  -- No Values
	  ,tlam.descriptions AS BriefLegal -- From tblleaseAbstractMapping
	  ,@currentDateTime AS EntryDate -- Default Value
	  ,NULLIF(CAST('' AS INT),'') AS oldLandID -- No Values
	  ,@currentDateTime AS _CreatedDateTime -- Default Value
	  ,'LND-6838' AS _CreatedBy -- Default Value
	  ,@currentDateTime AS _ModifiedDateTime -- Default Value
	  ,'LND-6838' AS _ModifiedBy -- Default Value
	  ,@currentDateTime AS AutoGenDate -- Default Value
	  ,NULLIF(CAST('' AS CHAR(50)),'') AS PatentNumber -- Default Values
	  ,NULLIF(CAST('' AS CHAR(50)),'') AS PatentVolume  -- Default Values
	  ,NULLIF(CAST('' AS CHAR(50)),'') AS CertificateNumber  -- Default Values
	  ,NULLIF(CAST('' AS CHAR(50)),'') AS FileNumber  -- Default Values
	  ,NULLIF(CAST('' AS CHAR(10)),'') AS PlatVolumeCabinet  -- Default Values
	  ,NULLIF(CAST('' AS CHAR(10)),'') AS PlatPageSlide  -- Default Values
	  ,NULLIF(CAST('' AS CHAR(20)),'') AS PlatBookType  -- Default Values
	  ,NULLIF(CAST('' AS CHAR(500)),'') AS QuarterCallsFull  -- Parsed From BriefLegals
	  ,NULLIF(CAST('' AS CHAR(100)),'') AS NewCityBlock  -- Parsed From BriefLegals
	  ,NULLIF(CAST('' AS INT),'') AS SubdivisionNameId  -- Default Values
	  ,CAST(0 AS BIT) AS IsDeleted  -- Default Value
	  ,tlc.CountyName AS CountyName
	  ,tls.StateName AS StateName
INTO countyScansTitle.dbo.LND_6838_tbllandDescription
FROM [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] tll
JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLeaseDocumentMapping] tlldm ON tlldm.leaseid = tll.leaseid
JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblleaseAbstractMapping] tlam ON tlam.mappingid = tlldm.legalleasedocumentmappingid
JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblAbstract] ta ON ta.abstractid = tlam.abstractid
-- Change this so it uses the newly created staging tables
JOIN countyScansTitle.dbo.LND_6732_tblexportLog tel ON tel.LeaseID = tll.leaseid
JOIN countyScansTitle.dbo.LND_6732_tblrecord tr ON tr.recordID = tel.recordID
JOIN countyScansTitle.dbo.tbllookupStates tls ON tls.StateID = tr.stateID
JOIN countyScansTitle.dbo.tbllookupCounties tlc ON tlc.CountyID = tr.countyID
WHERE tlam.descriptions != ''
AND tlam.descriptions IS NOT NULL 
AND tlam.descriptions NOT LIKE '%ALL%'
AND tlam.descriptions NOT LIKE '%BELOW%'
AND tlam.descriptions NOT LIKE '%REMARKS%'
AND tls.StateName NOT IN ('PA','WV','OH');


SELECT *
FROM countyScansTitle.dbo.LND_6838_tbllandDescription

ALTER TABLE countyScansTitle.dbo.LND_6838_tbllandDescription
ALTER COLUMN _CreatedBy VARCHAR(75);

ALTER TABLE countyScansTitle.dbo.LND_6838_tbllandDescription
ALTER COLUMN _ModifiedBy VARCHAR(75);

SELECT TOP 10000 *
FROM countyScansTitle.dbo.LND_6838_tbllandDescription

-- Gather records for brief-legals-parser -> CSV
/*  -- Copy INPUT CSV -> brief-legals-parser/python_modules/input_data, OUTPUT -> C:\tmp\BriefLegals\parsed_brief_legals.csv
SELECT landDescriptionID, CountyName AS County, StateName AS State, BriefLegal
FROM countyScansTitle.dbo.LND_6838_tbllandDescription
ORDER BY CountyName;
*/


-- Explode Section Values
-- Create a temporary table to store the split sections
SET XACT_ABORT ON;
BEGIN TRAN;

IF OBJECT_ID(N'tempdb..#SplitSections', 'U') IS NOT NULL
	DROP TABLE #SplitSections;

CREATE TABLE #SplitSections (
    landDescriptionId NVARCHAR(40),
	recordID VARCHAR(36),
	CountyID INT,
	Subdivision CHAR(300),
	Survey CHAR(300),
	Lot CHAR(100),
	Block CHAR(100),
    OriginalSection NVARCHAR(255),
    BriefLegal NVARCHAR(MAX),
    SectionValue NVARCHAR(255),
	Township CHAR(50),
	RangeOrBLock VARCHAR(22),
	AbstractName INT,
	Suffix CHAR(50),
	QuarterCalls CHAR(500),
	EntryDate DATETIME, 
	oldLandID INT, 
	_CreatedDateTime DATETIME, 
	_CreatedBY VARCHAR(75), 
	_ModifiedDateTime DATETIME, 
	_ModifiedBy VARCHAR(75), 
	AutoGenDate DATETIME, 
	SubdivisionNameId INT, 
	IsDeleted BIT
);

-- Insert the split values into the temporary table
INSERT INTO #SplitSections (landDescriptionId, recordID, CountyID, Subdivision, Survey, Lot, Block, OriginalSection, BriefLegal, SectionValue, Township, RangeOrBlock, AbstractName, Suffix, QuarterCalls, EntryDate, oldLandID, _CreatedDateTime, _CreatedBY, _ModifiedDateTime, _ModifiedBy, AutoGenDate, SubdivisionNameId, IsDeleted)
SELECT 
    ld.landDescriptionId,
	ld.recordID,
	ld.CountyID,
	ld.Subdivision,
	ld.Survey,
	ld.Lot,
	ld.Block,
    ld.Section, 
    ld.BriefLegal,
    LTRIM(RTRIM(s.value)) AS SectionValue,
	ld.Township,
	ld.RangeOrBlock,
	ld.AbstractName,
	ld.Suffix,
	ld.QuarterCalls,
	ld.EntryDate,
	ld.oldLandID,
	ld._CreatedDateTime,
	ld._CreatedBy,
	ld._ModifiedDateTime,
	ld._ModifiedBy,
	ld.AutoGenDate,
	ld.SubdivisionNameId,
	ld.IsDeleted
FROM countyScansTitle.dbo.LND_6838_tbllandDescription ld
CROSS APPLY STRING_SPLIT(ld.Section, ',') s
WHERE ld.Section LIKE '%,%';

SELECT *
FROM #SplitSections

-- Delete the records with multiple sections from the original table
DELETE FROM countyScansTitle.dbo.LND_6838_tbllandDescription
WHERE Section LIKE '%,%';

-- Insert the split records back into the original table
INSERT INTO countyScansTitle.dbo.LND_6838_tbllandDescription (
    landDescriptionId,
	recordID,
	CountyID,
	Subdivision,
	Survey,
	Lot,
	Block,
    Section,
	Township,
	RangeOrBlock,
	AbstractName,
	Suffix,
	QuarterCalls,
    BriefLegal,
	EntryDate,
	oldLandID,
	_CreatedDateTime,
	_CreatedBy,
	_ModifiedDateTime,
	_ModifiedBy,
	AutoGenDate,
	SubdivisionNameId,
	IsDeleted
)
SELECT 
    ss.landDescriptionId,
	ss.recordID,
	ss.CountyID,
	ss.Subdivision,
	ss.Survey,
	ss.Lot,
	ss.Block,
    ss.SectionValue,
	ss.Township,
	ss.RangeOrBlock,
	ss.AbstractName,
	ss.Suffix,
	ss.QuarterCalls,
    ss.BriefLegal,
	ss.EntryDate,
	ss.oldLandID,
	ss._CreatedDateTime,
	ss._CreatedBy,
	ss._ModifiedDateTime,
	ss._ModifiedBy,
	ss.AutoGenDate,
	ss.SubdivisionNameId,
	ss.IsDeleted
FROM #SplitSections ss;

-- Drop the temporary table when finished
DROP TABLE #SplitSections;

SELECT TOP 1000 *
FROM countyScansTitle.dbo.LND_6838_tbllandDescription ld
WHERE Section IS NOT NULL

SELECT TOP 1000 *
FROM countyScansTitle.dbo.LND_6838_tbllandDescription ld
CROSS APPLY STRING_SPLIT(ld.Section, ',') s
WHERE ld.Section LIKE '%,%';


-- Rollback the transaction (for testing purposes)
--ROLLBACK TRAN;
-- Uncomment the following line to commit the transaction
COMMIT TRAN;

-- Check transaction count and state
SELECT @@TRANCOUNT, XACT_STATE();



-- Explode QuarterCalls Values
-- Create a temporary table to store the split QuarterCalls
SET XACT_ABORT ON;
BEGIN TRAN;

IF OBJECT_ID(N'tempdb..#SplitQuarterCalls', 'U') IS NOT NULL
	DROP TABLE #SplitQuarterCalls;

CREATE TABLE #SplitQuarterCalls (
    landDescriptionID NVARCHAR(40),
	recordID VARCHAR(36),
	CountyID INT,
	Subdivision CHAR(300),
	Survey CHAR(300),
	Lot CHAR(100),
	Block CHAR(100),
	Section CHAR(50),
	Township CHAR(50),
	RangeOrBlock VARCHAR(22),
	AbstractName INT,
	Suffix CHAR(50),
	QuarterCallValue NVARCHAR(255),
	QuarterCalls CHAR(500),
    BriefLegal NVARCHAR(MAX),
	EntryDate DATETIME, 
	oldLandID INT, 
	_CreatedDateTime DATETIME, 
	_CreatedBy VARCHAR(75), 
	_ModifiedDateTime DATETIME, 
	_ModifiedBy VARCHAR(75), 
	AutoGenDate DATETIME, 
	SubdivisionNameId INT, 
	IsDeleted BIT
);

-- Insert the split values into the temporary table
INSERT INTO #SplitQuarterCalls (
	landDescriptionId, recordID, CountyID, Subdivision, Survey, Lot, Block, 
	Section, Township, RangeOrBlock, AbstractName, Suffix, QuarterCallValue, QuarterCalls,
	BriefLegal, EntryDate, oldLandID, _CreatedDateTime, _CreatedBy, 
	_ModifiedDateTime, _ModifiedBy, AutoGenDate, SubdivisionNameId, IsDeleted)
SELECT 
    ld.landDescriptionId,
	ld.recordID,
	ld.CountyID,
	ld.Subdivision,
	ld.Survey,
	ld.Lot,
	ld.Block,
	ld.Section,
	ld.Township,
	ld.RangeOrBlock,
	ld.AbstractName,
	ld.Suffix,
    LTRIM(RTRIM(s.value)) AS QuarterCallValue,
    ld.QuarterCalls, 
    ld.BriefLegal,
	ld.EntryDate,
	ld.oldLandID,
	ld._CreatedDateTime,
	ld._CreatedBy,
	ld._ModifiedDateTime,
	ld._ModifiedBy,
	ld.AutoGenDate,
	ld.SubdivisionNameId,
	ld.IsDeleted
FROM countyScansTitle.dbo.LND_6838_tbllandDescription ld
CROSS APPLY STRING_SPLIT(ld.QuarterCalls, ',') s
WHERE ld.QuarterCalls LIKE '%,%';

SELECT *
FROM #SplitQuarterCalls
ORDER BY landDescriptionID

-- Delete the records with multiple QuarterCalls from the original table
DELETE FROM countyScansTitle.dbo.LND_6838_tbllandDescription
WHERE QuarterCalls LIKE '%,%';

-- Insert the split records back into the original table
INSERT INTO countyScansTitle.dbo.LND_6838_tbllandDescription (
    landDescriptionId, recordID, CountyID, Subdivision, Survey, Lot, Block, 
    Section, Township, RangeOrBlock, AbstractName, Suffix, 
    QuarterCalls, BriefLegal, EntryDate, oldLandID, _CreatedDateTime, 
    _CreatedBy, _ModifiedDateTime, _ModifiedBy, AutoGenDate, SubdivisionNameId, IsDeleted
)
SELECT 
    sq.landDescriptionId,
	sq.recordID,
	sq.CountyID,
	sq.Subdivision,
	sq.Survey,
	sq.Lot,
	sq.Block,
	sq.Section,
	sq.Township,
	sq.RangeOrBlock,
	sq.AbstractName,
	sq.Suffix,
	sq.QuarterCallValue,
    sq.BriefLegal,
	sq.EntryDate,
	sq.oldLandID,
	sq._CreatedDateTime,
	sq._CreatedBy,
	sq._ModifiedDateTime,
	sq._ModifiedBy,
	sq.AutoGenDate,
	sq.SubdivisionNameId,
	sq.IsDeleted
FROM #SplitQuarterCalls sq;

-- Drop the temporary table when finished
DROP TABLE #SplitQuarterCalls;

SELECT TOP 1000 *
FROM countyScansTitle.dbo.LND_6838_tbllandDescription ld
WHERE QuarterCalls IS NOT NULL

SELECT *
FROM countyScansTitle.dbo.LND_6838_tbllandDescription ld
CROSS APPLY STRING_SPLIT(ld.QuarterCalls, ',') s
WHERE ld.QuarterCalls LIKE '%,%';

-- Rollback the transaction (for testing purposes)
--ROLLBACK TRAN;
-- Uncomment the following line to commit the transaction
COMMIT TRAN;

-- Check transaction count and state
SELECT @@TRANCOUNT, XACT_STATE();


-- Explode Lot Values
SET XACT_ABORT ON;
BEGIN TRAN;

IF OBJECT_ID(N'tempdb..#SplitLots', 'U') IS NOT NULL
	DROP TABLE #SplitLots;

CREATE TABLE #SplitLots (
    landDescriptionID NVARCHAR(40),
	recordID VARCHAR(36),
	CountyID INT,
	Subdivision CHAR(300),
	Survey CHAR(300),
	Lot CHAR(100),
	Block CHAR(100),
	Section CHAR(50),
	Township CHAR(50),
	RangeOrBlock VARCHAR(22),
	AbstractName INT,
	Suffix CHAR(50),
	QuarterCalls CHAR(500),
    BriefLegal NVARCHAR(MAX),
	EntryDate DATETIME, 
	oldLandID INT, 
	_CreatedDateTime DATETIME, 
	_CreatedBy VARCHAR(75), 
	_ModifiedDateTime DATETIME, 
	_ModifiedBy VARCHAR(75), 
	AutoGenDate DATETIME, 
	SubdivisionNameId INT, 
	IsDeleted BIT
);

-- Insert the split values into the temporary table
INSERT INTO #SplitLots (
	landDescriptionId, recordID, CountyID, Subdivision, Survey, Lot, Block, 
	Section, Township, RangeOrBlock, AbstractName, Suffix, QuarterCalls,
	BriefLegal, EntryDate, oldLandID, _CreatedDateTime, _CreatedBy, 
	_ModifiedDateTime, _ModifiedBy, AutoGenDate, SubdivisionNameId, IsDeleted)
SELECT 
    ld.landDescriptionId,
	ld.recordID,
	ld.CountyID,
	ld.Subdivision,
	ld.Survey,
	LTRIM(RTRIM(lotSplit.value)) AS Lot,
	ld.Block,
	ld.Section,
	ld.Township,
	ld.RangeOrBlock,
	ld.AbstractName,
	ld.Suffix,
    ld.QuarterCalls, 
    ld.BriefLegal,
	ld.EntryDate,
	ld.oldLandID,
	ld._CreatedDateTime,
	ld._CreatedBy,
	ld._ModifiedDateTime,
	ld._ModifiedBy,
	ld.AutoGenDate,
	ld.SubdivisionNameId,
	ld.IsDeleted
FROM countyScansTitle.dbo.LND_6838_tbllandDescription ld
CROSS APPLY STRING_SPLIT(ld.Lot, ',') lotSplit
WHERE ld.Lot LIKE '%,%';

SELECT *
FROM #SplitLots
ORDER BY landDescriptionID;

-- Delete the records with multiple Lots from the original table
DELETE FROM countyScansTitle.dbo.LND_6838_tbllandDescription
WHERE Lot LIKE '%,%';

-- Insert the split records back into the original table
INSERT INTO countyScansTitle.dbo.LND_6838_tbllandDescription (
    landDescriptionId, recordID, CountyID, Subdivision, Survey, Lot, Block, 
    Section, Township, RangeOrBlock, AbstractName, Suffix, 
    QuarterCalls, BriefLegal, EntryDate, oldLandID, _CreatedDateTime, 
    _CreatedBy, _ModifiedDateTime, _ModifiedBy, AutoGenDate, SubdivisionNameId, IsDeleted
)
SELECT 
    sq.landDescriptionId,
	sq.recordID,
	sq.CountyID,
	sq.Subdivision,
	sq.Survey,
	sq.Lot,
	sq.Block,
	sq.Section,
	sq.Township,
	sq.RangeOrBlock,
	sq.AbstractName,
	sq.Suffix,
	sq.QuarterCalls,
    sq.BriefLegal,
	sq.EntryDate,
	sq.oldLandID,
	sq._CreatedDateTime,
	sq._CreatedBy,
	sq._ModifiedDateTime,
	sq._ModifiedBy,
	sq.AutoGenDate,
	sq.SubdivisionNameId,
	sq.IsDeleted
FROM #SplitLots sq;

-- Drop the temporary table when finished
DROP TABLE #SplitLots;

SELECT TOP 1000 *
FROM countyScansTitle.dbo.LND_6838_tbllandDescription ld
WHERE Lot IS NOT NULL;

SELECT *
FROM countyScansTitle.dbo.LND_6838_tbllandDescription ld
CROSS APPLY STRING_SPLIT(ld.Lot, ',') lotSplit
WHERE ld.Lot LIKE '%,%';

-- Rollback the transaction (for testing purposes)
--ROLLBACK TRAN;
-- Uncomment the following line to commit the transaction
COMMIT TRAN;

-- Check transaction count and state
SELECT @@TRANCOUNT, XACT_STATE();


-- QA Section
SELECT COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription


-- QA'ed, Subdivision has varying lengths of whitespace, should this be normalized?
SELECT DISTINCT Subdivision, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription
GROUP BY Subdivision
ORDER BY COUNT(*) DESC

-- QA'ed
SELECT DISTINCT Survey, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription
GROUP BY Survey
ORDER BY COUNT(*) DESC

-- QA'ed
SELECT DISTINCT Lot, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription
GROUP BY Lot
ORDER BY COUNT(*) DESC

-- QA'ed
SELECT DISTINCT Block, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription
GROUP BY Block
ORDER BY COUNT(*) DESC

-- Section contains values with Comma's
-- Need to create a new row for each section
-- Section needs to be a float
SELECT DISTINCT Section, BriefLegal, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription
WHERE Section LIKE '%,%'
GROUP BY Section, BriefLegal
ORDER BY COUNT(*) DESC

-- QA'ed
SELECT DISTINCT Township, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription
GROUP BY Township
ORDER BY COUNT(*) DESC

-- QA'ed
SELECT DISTINCT RangeOrBlock, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription
GROUP BY RangeorBlock
ORDER BY COUNT(*) DESC

-- QA'ed
SELECT DISTINCT AbstractName, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription
GROUP BY AbstractName
ORDER BY COUNT(*) DESC

-- No Records
SELECT DISTINCT Suffix, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription
GROUP BY Suffix
ORDER BY COUNT(*) DESC

-- Verify with Lindsey whether we should explode the quartercalls
-- Check other columns that might need to be exploded
SELECT DISTINCT QuarterCalls, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription
GROUP BY QuarterCalls
ORDER BY COUNT(*) DESC

-- Need to update the QuarterCallsFull if QuarterCalls IS NOT NULL
SELECT DISTINCT QuarterCallsFull, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription
GROUP BY QuarterCallsFull
ORDER BY COUNT(*) DESC


-- No Records
SELECT DISTINCT AcreageByTract, COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription
GROUP BY AcreageByTract
ORDER BY COUNT(*) DESC



-- Total Count: 1,054,161
-- QuaterCalls Count: 520,824
SELECT COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription
WHERE QuarterCalls != ''

-- Total Count: 1,054,161
-- Subdivision Count: 325,786
SELECT COUNT(*)
FROM countyScansTitle.dbo.LND_6838_tbllandDescription
WHERE Subdivision != ''


SELECT BriefLegal, *
FROM countyScansTitle.dbo.LND_6838_tbllandDescription
WHERE Subdivision = ''
AND Survey = ''
AND Lot = ''
AND Block = ''
AND Section = ''
AND Township = ''
AND Suffix = ''
AND QuarterCalls = ''

SELECT BriefLegal, *
FROM countyScansTitle.dbo.LND_6838_tbllandDescription
WHERE Subdivision != ''
OR Survey != ''
OR Lot != ''
OR Block != ''
OR Section != ''
OR Township != ''
OR Suffix != ''
OR QuarterCalls != ''
ORDER BY QuarterCalls


-- Create query to change Section column to Float, can use TRY_CAST
-- Lindsey recommends moving these into QuarterCalls/Lots/etc.
-- 
select BriefLegal, count(*)
from countyScansTitle.dbo.LND_6838_tbllandDescription
where BriefLegal is not null 
and subdivision = '' 
and Survey = '' 
and AbstractName is null 
and lot = ''
and block = '' 
and section = '' 
and Township = '' 
and RangeOrBlock = '' 
and AcreageByTract = '' 
and QuarterCalls = ''
group by BriefLegal
order by count(*) desc

