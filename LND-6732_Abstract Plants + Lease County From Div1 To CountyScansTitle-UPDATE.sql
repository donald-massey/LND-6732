USE countyScansTitle;
SET ANSI_NULLS ON;
SET ANSI_WARNINGS ON;
GO
/****************************************************************************************************************************************************************
** File:            LND-6732_Categorized_Queries_Updated.sql
** Author:          Donald Massey (Updated)
** Copyright:       Enverus
** Creation Date:   2025-03-19
** Description:     Categorized queries for recordID existence in tblexportlog and tblrecord using base query structure
***************************************************************************************************************************************************************
** Date:        Author:             Description:
** --------     --------            -------------------------------------------
** 2025-03-19   Donald Massey       Updated to categorize records based on table presence using specified base query
***************************************************************************************************************************************************************/

-- Create temporary tables to store DIV1 and CountyScansTitle data
IF OBJECT_ID('tempdb..#LND_6732_cst_values', 'U') IS NOT NULL
    DROP TABLE #LND_6732_cst_values;

-- Create CST values temp table
SELECT 
    LOWER(tr.recordID) AS recordID,
    tr.recordNumber,
    CONVERT(varchar, tr.fileDate, 23) AS fileDate,
	CASE
			WHEN TRY_CONVERT(INT, ISNULL(tr.volume,'')) = 0
			THEN NULL
			ELSE TRY_CONVERT(INT, ISNULL(tr.volume,''))
	END AS volume,
	CASE
			WHEN TRY_CONVERT(INT, ISNULL(tr.page,'')) = 0
			THEN NULL
			ELSE TRY_CONVERT(INT, ISNULL(tr.page,''))
   END AS page,
   c.CountyName AS countyName,
   tr.countyID AS countyID
INTO #LND_6732_cst_values
FROM countyScansTitle.dbo.tblrecord tr
JOIN countyScansTitle.Tracker.MasterCountyLookup mcl ON mcl.LeasingID = tr.countyID
JOIN countyScansTitle.dbo.tbllookupCounties c ON c.countyID = tr.countyID AND c.stateID = tr.stateID
WHERE tr.recordIsLease = 1
  AND tr.statusID IN (4,16)
  AND tr.fileDate >= '2002-01-01'
	      -- Include EOG McMullen and Gonzales. These are only keyed for EOG so we need to sources leases from those plants.
              and tr.countyID not in (288,291,292,293,295,296,298,300,684,685,686,687,688,689,690,691,692,693,694,695,696,697,698,699,
              700,701,702,703,704,705,706,707,708,709,710,711,712,713,714,715,716,1187);
-- 958,244
SELECT COUNT(*) FROM #LND_6732_cst_values;


-- Create DIV1 values temp table
IF OBJECT_ID('tempdb..#LND_6732_div1_values', 'U') IS NOT NULL
    DROP TABLE #LND_6732_div1_values;

SELECT 
    tll.LeaseID,
	LOWER(NEWID()) AS div1_generated_recordid,
	tll.instrument,
    tll.recordNumber,
    CONVERT(varchar, tll.recordDate, 23) AS RecordDate,
	CASE
		WHEN TRY_CONVERT(INT, ISNULL(tll.Vol,'')) = 0
		THEN NULL
		ELSE TRY_CONVERT(INT, ISNULL(tll.Vol,''))
	END AS volume,
	CASE
		WHEN TRY_CONVERT(INT, ISNULL(tll.pg,'')) = 0
		THEN NULL
		ELSE TRY_CONVERT(INT, ISNULL(tll.pg,''))
	END AS page,
    CASE
		WHEN TDF.Filename IS NOT NULL
		THEN CONCAT('\\smb.dc2isilon.na.drillinginfo.com\didocuments\leaseTracts\', tdf.Path, '\', tdf.Filename)
		ELSE NULL
	END AS sourceFilePath,
	tdf.Filename AS originalFileName,
	CASE
		WHEN tdf.Filename LIKE ('%.pdf%')
			THEN '.pdf'
		WHEN tdf.Filename LIKE ('%.jpg%')
			THEN '.jpg'
		WHEN tdf.Filename LIKE ('%.tif%')
			THEN '.tif'
		ELSE NULL
	END AS fileType,
	CASE
		WHEN tdf.Filename LIKE ('%.pdf%')
			THEN '.pdf'
		WHEN tdf.Filename LIKE ('%.jpg%')
			THEN '.jpg'
		WHEN tdf.Filename LIKE ('%.tif%')
			THEN '.tif'
		ELSE NULL
	END AS fileExtension,
    c.CountyName AS CountyName,
	tll.countyID AS countyID,
	c.StateID AS StateID,
	EffectiveDate,
	InstrumentDate,
	acres,
	tll.depthcode,
	depthMin,
	depthMax,
	extensionTermMonths,
	extensionBonus,
	isBlm,
	isState,
	TermMonths,
	ROYALTY,
	DelayRental,
	DelayRentalValue
INTO #LND_6732_div1_values
FROM [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] tll
LEFT JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblCounty] c ON c.CountyID = tll.CountyID
LEFT JOIN [AUS2-DIV1-DDB01].[div1_Daily].[dbo].[tblLegalLeaseDocumentMapping] tlldm ON tlldm.leaseId = tll.LeaseID
LEFT JOIN [AUS2-DIV1-DDB01].[div1_Daily].[dbo].[tblDocument] td ON td.DocumentId = tlldm.documentId
LEFT JOIN [AUS2-DIV1-DDB01].[div1_Daily].[dbo].[tblDocumentFile] tdf ON tdf.DocumentId = td.DocumentId
LEFT JOIN countyScansTitle.Tracker.MasterCountyLookup mcl ON mcl.leasingID = tll.CountyID;

-- 3,058,774
SELECT COUNT(*)
FROM #LND_6732_div1_values

SET XACT_ABORT ON;
BEGIN TRAN;

-- CATEGORY 1: recordid exists in tblrecord and tblexportlog with NULL LeaseID and DIV1 Match
-- 21,840 records
SELECT DISTINCT
    LOWER(cst.recordID) AS cst_recordID,
    cst.recordNumber AS cst_recordno,
    div1.LeaseID AS div1_leaseid,
    div1.recordNumber AS div1_recordno,
    cst.volume AS cst_volume,
    cst.page AS cst_page,
    div1.volume AS div1_volume,
    div1.page AS div1_page,
    div1.CountyName AS div1_countyName,
	'Category 1: Exists in countyScansTitle with NULL LeaseID' AS category
FROM countyScansTitle.dbo.tblexportLog tel
JOIN #LND_6732_cst_values cst ON tel.recordID = cst.recordID
JOIN #LND_6732_div1_values div1 ON div1.recordNumber = cst.recordNumber
                               AND div1.RecordDate = cst.fileDate
                               AND div1.CountyName = cst.countyName
WHERE tel.LeaseID IS NULL;

-- Update statement for Category 1 records
UPDATE tel
SET 
    tel.LeaseID = div1.LeaseID,
	tel.zipName = 'LND-6732(1)',
    tel._ModifiedDateTime = GETDATE()
FROM countyScansTitle.dbo.tblexportLog tel
JOIN #LND_6732_cst_values cst ON tel.recordID = cst.recordID
JOIN #LND_6732_div1_values div1 ON div1.recordNumber = cst.recordNumber
                               AND div1.RecordDate = cst.fileDate
                               AND div1.CountyName = cst.countyName
WHERE tel.LeaseID IS NULL AND (_CreatedBy IS NOT NULL AND _ModifiedBy IS NOT NULL);

UPDATE tel
SET 
    tel.LeaseID = div1.LeaseID,
	tel.zipName = 'LND-6732(1)',
	tel._CreatedBy = 'LND-6732(1)',
    tel._ModifiedDateTime = GETDATE(),
	tel._ModifiedBy = 'LND-6732(1)'
FROM countyScansTitle.dbo.tblexportLog tel
JOIN #LND_6732_cst_values cst ON tel.recordID = cst.recordID
JOIN #LND_6732_div1_values div1 ON div1.recordNumber = cst.recordNumber
                               AND div1.RecordDate = cst.fileDate
                               AND div1.CountyName = cst.countyName
WHERE tel.LeaseID IS NULL AND (_CreatedBy IS NULL AND _ModifiedBy IS NULL);


SELECT COUNT(DISTINCT tel.recordID)
FROM countyScansTitle.dbo.tblexportLog tel
JOIN #LND_6732_cst_values cst ON tel.recordID = cst.recordID
JOIN #LND_6732_div1_values div1 ON div1.recordNumber = cst.recordNumber
                               AND div1.RecordDate = cst.fileDate
                               AND div1.CountyName = cst.countyName
WHERE zipName = 'LND-6732(1)';



-- CATEGORY 2: Records in Div1 that don't have a match in CST
-- 2,118,373 Records
SELECT 
    cst.recordID AS cst_recordID,
    cst.recordNumber AS cst_recordno,
    tel.leaseID AS tel_leaseID,
	div1.LeaseID AS div1_leaseid,
	div1_generated_recordid,
    div1.recordNumber AS div1_recordno,
    cst.volume AS cst_volume,
    cst.page AS cst_page,
    div1.volume AS div1_volume,
    div1.page AS div1_page,
    div1.CountyName AS div1_countyName,
    'Category 2: Exists in Div1 but not in CST' AS category
FROM #LND_6732_div1_values div1
LEFT JOIN countyScansTitle.dbo.tblexportlog tel ON tel.LeaseID = div1.LeaseID
LEFT JOIN #LND_6732_cst_values cst ON cst.recordNumber = div1.RecordNumber 
							 AND cst.fileDate = div1.RecordDate
							 AND cst.countyName = div1.CountyName
JOIN countyScansTitle.Tracker.MasterCountyLookup mcl ON mcl.LeasingID = div1.countyID
WHERE tel.LeaseID IS NULL AND mcl.CourthouseTitleID IS NOT NULL;


-- Create New Status In tblStatus
INSERT INTO countyScansTitle.dbo.tblStatus
(statusID, stapleDescription, dataEntryDescription, IncomingInspectionDescription, assignmentDescription)
SELECT
	90,								-- statusID
	NULL,							-- stapleDescription
	'Leases Mapped From DIV1',		-- dataEntryDescription
	'not currently in use',			-- IncomingInspectionDescription
	'not currently in use'			-- assignmentDescription

-- Set Updated Records To StatusID = 90
UPDATE tr
SET tr.statusID = 90
FROM countyScansTitle.dbo.tblrecord tr
JOIN countyScansTitle.dbo.tblexportlog tel ON tr.recordID = tel.recordID
WHERE tel.zipName = 'LND-6732(1)'


-- INSERT into tblrecord for Category 2 records
-- Default Value (Gathered from Ryan M Mapping / csdigital-to-cstitle)
-- 2,118,373 Records
INSERT INTO countyScansTitle.dbo.tblrecord (
    recordID,
	sourceFilePath,
	storageFilePath,
	originalFileName,
	fileType,
	fileExtension,
	gathererID,
	receivedDate,
	bookTypeID,
    countyID,
	stateID,
    volume,
    page,
	outsourceID,
	outsourcedDate,
	stapled,
	stapleInspectorNumber,
	stapleQcDate,
	statusID,
    keyerNumber,
    reassignedTo,
    keyedDate,
    modifiedDate,
    inspectorNumber,
    qcDate,
    manualAccept,
    manualReject,
    qaReserved,
    holding,
    keyerComments,
    supervisorComments,
    pendingDate,
    exported,
    exportDate,
    RecordDate,                    
    effectiveDate,
    instrumentTypeID,
	recordNumber,
	instrumentDate,
	fileDate,
    remarks,                    
	MultipleLeasesForTract,
	AreaAmount,
	AreaNotAvailable,
	LandDescriptionNotAvailable,
	DepthsCoveredType,
    DepthsCoveredFrom,             
    DepthsCoveredTo,               
    Extension,
    ExtensionLength,
    ExtensionType,
    ExtensionBonus,
    ExtensionBLM,
    ExtensionState,
    TermLength,
    TermType,
    TermAvailable,
    RoyaltyAmount,
    DelayRental,
    RentalAmount,
    RentalBonus,
    DRVolumePage,
    DRRecordNumber,
    needsRedaction,
    ExportedToWeb2,
    ExportedToWeb2Date,
    ExportedToWeb,
    ExportedToWebDate,
    FAQDate,
    FAQAnsweredDate,
    FAQAnsweredBy,
    PoorImage,
    NoPriorReference,
    MIP,
    HandWritten,
    _CreatedDateTime,
    _CreatedBy,
    _ModifiedDateTime,
    _ModifiedBy,
    causeNumber,
    qcBatchID,
    auditBatchID,
    IsWebDoc,
    SiteId,
    Url,
    recordIsLease,
    recordIsCourthouse,
    recordIsAssignment,
    prevQCBID,
    ParcelNumber,
    InstrumentTypeFull,
    TotalPages,
    Consideration,
    InstrumentTypeFullId)
SELECT 
    div1.div1_generated_recordid,					-- recordID, Default Value
    CASE
		WHEN div1.sourceFilePath IS NOT NULL
		THEN div1.sourceFilePath
		ELSE ''
	END AS sourceFilePath,							-- sourceFilePath, DIV1
	'',												-- storageFilePath, TBD
	CASE
		WHEN div1.originalFileName IS NOT NULL
		THEN div1.originalFileName
		ELSE ''
	END AS originalFileName,						-- originalFileName, DIV1
	CASE
		WHEN div1.fileType IS NOT NULL
		THEN div1.fileType
		ELSE ''
	END AS fileType,								-- fileType, DIV1
	CASE
		WHEN div1.fileExtension IS NOT NULL
		THEN div1.fileExtension
		ELSE ''
	END AS fileExtension,							-- fileExtension, DIV1
	10,	        									-- gathererID, Default Value
	GETDATE(),	     								-- receivedDate, Default Value
	17,						     					-- bookTypeID, Default Value
    mcl.courthouseTitleID,   						-- countyID, Default Value
	CASE
		WHEN div1.StateID = 1
		THEN 48
		WHEN div1.StateID = 2
		THEN 22
		WHEN div1.StateID = 3
		THEN 55
		WHEN div1.StateID = 4
		THEN 35
		WHEN div1.StateID = 5
		THEN 40
		WHEN div1.StateID = 60
		THEN 1
		WHEN div1.StateID = 62
		THEN 5
		WHEN div1.StateID = 63
		THEN 7
		WHEN div1.StateID = 64
		THEN 8
		WHEN div1.StateID = 66
		THEN 10
		WHEN div1.StateID = 74
		THEN 20
		WHEN div1.StateID = 79
		THEN 26
		WHEN div1.StateID = 81
		THEN 28
		WHEN div1.StateID = 83
		THEN 30
		WHEN div1.StateID = 85
		THEN 32
		WHEN div1.StateID = 90
		THEN 38
		WHEN div1.StateID = 91
		THEN 39
		WHEN div1.StateID = 92
		THEN 41
		WHEN div1.StateID = 93
		THEN 43
		WHEN div1.StateID = 96
		THEN 46
		WHEN div1.StateID = 98
		THEN 49
		WHEN div1.StateID = 101
		THEN 52
		WHEN div1.StateID = 102
		THEN 53
	END AS stateID,									-- stateID, Default Value
	div1.volume,									-- Volume, DIV1
    div1.page,										-- Page, DIV1
	10,   											-- outsourceID, Default Value
	NULL,											-- outsourcedDate, Default Value
	1,	    										-- stapled, Default Value
    NULL,											-- stapleInspectorNumber, Default Value
    NULL,											-- stapleQcDate, Default Value
	90,     										-- statusID, Default Value
	NULL,											-- keyerNumber, Default Value
    NULL,											-- reassignedTo, Default Value
    NULL,											-- keyedDate, Default Value
    NULL,											-- modifiedDate, Default Value
    NULL,											-- inspectorNumber, Default Value
    NULL,											-- qcDate, Default Value
    0,      										-- manualAccept, Default Value
    0,      										-- manualReject, Default Value
    0,      										-- qaReserved, Default Value
    0,      										-- holding, Default Value
    NULL,											-- keyerComments, Default Value
    NULL,											-- supervisorComments, Default Value
    NULL,											-- pendingDate, Default Value
    0,      										-- exported, Default Value
    NULL,											-- exportDate, Default Value
    div1.RecordDate,								-- recordDate, DIV1
    div1.EffectiveDate,      						-- effectiveDate, DIV1
	CASE
		WHEN div1.instrument = 0
		THEN 'OGL'
		WHEN div1.instrument = 1
		THEN 'MOGL'
		WHEN div1.instrument = 2
		THEN 'OP'
		WHEN div1.instrument = 3
		THEN 'SOP'
		WHEN div1.instrument = 4
		THEN 'SMEM'
		WHEN div1.instrument = 5
		THEN 'REOGL'
		WHEN div1.instrument = 6
		THEN 'OGL'
		WHEN div1.instrument = 7
		THEN 'OGLEXT'
		WHEN div1.instrument = 8
		THEN 'SPER'
		WHEN div1.instrument = 9
		THEN 'ROGL'
		WHEN div1.instrument = 10
		THEN 'OGLAMD'
		WHEN div1.instrument = 11
		THEN 'ASN'
		WHEN div1.instrument = 12
		THEN 'MD'
		WHEN div1.instrument = 13
		THEN 'RD'
	END AS instrumentTypeID,						-- instrumentTypeID, DIV1
	div1.recordNumber,								-- recordNumber, DIV1
    div1.InstrumentDate,							-- instrumentDate, DIV1
	div1.RecordDate,								-- fileDate, DIV1
    'LND-6732(2)',									-- remarks, DIV1
    0,			    								-- MultipleLeasesForTract, Default Value
    CAST(div1.acres AS FLOAT),						-- AreaAmount, DIV1
    CASE
		WHEN div1.acres IS NULL
		THEN -1
		ELSE 0
	END AS AreaNotAvailable,   						-- AreaNotAvailable, DIV1
    0,					     						-- LandDescriptionNotAvailable, Default Value
	CASE
		WHEN div1.depthcode = 0
		THEN 'All'
		WHEN div1.depthcode = 1
		THEN 'FromTo'
		WHEN div1.depthcode = 2
		THEN 'SurfaceTo'
		WHEN div1.depthcode = 3
		THEN 'DeeperThan'
		WHEN div1.depthcode = 255
		THEN 'No Data'
		ELSE NULL
	END AS DepthsCoveredType,						-- DepthsCoveredType, DIV1
	div1.depthMin,									-- DepthsCoveredFrom, DIV1
    div1.depthMax,									-- DepthsCoveredTo, DIV1
    CASE
		WHEN div1.extensionTermMonths = 0
		THEN 0
		ELSE 1
	END AS Extension,								-- Extension, DIV1
	div1.extensionTermMonths AS ExtensionLength,    -- ExtensionLength, DIV1
    CASE
        WHEN div1.extensionTermMonths >= 1
        THEN 'Months'
		ELSE NULL
	END AS ExtensionType,						    -- ExtensionType, Default Value
    CAST(div1.extensionBonus AS INT),				-- ExtensionBonus, DIV1
    div1.isBlm, 									-- ExtensionBLM, DIV1
    div1.isState, 									-- ExtensionState, DIV1
  	CASE
		WHEN TermMonths IS NULL
		THEN NULL
		WHEN TermMonths = 0
		THEN 0
		WHEN TermMonths BETWEEN 12 AND 23
		THEN 1
		WHEN TermMonths BETWEEN 24 AND 35
		THEN 2
		WHEN TermMonths BETWEEN 36 AND 47
		THEN 3
		WHEN TermMonths BETWEEN 48 AND 59
		THEN 4
		WHEN TermMonths BETWEEN 60 AND 71
		THEN 5
		WHEN TermMonths BETWEEN 72 AND 83
		THEN 6
		WHEN TermMonths BETWEEN 84 AND 95
		THEN 7
		WHEN TermMonths BETWEEN 96 AND 107
		THEN 8
		WHEN TermMonths BETWEEN 108 AND 119
		THEN 9
		WHEN TermMonths BETWEEN 120 AND 131
		THEN 10
		WHEN TermMonths BETWEEN 132 AND 143
		THEN 11
		WHEN TermMonths BETWEEN 144 AND 155
		THEN 12
		WHEN TermMonths BETWEEN 156 AND 167
		THEN 13
		WHEN TermMonths BETWEEN 168 AND 179
		THEN 14
		WHEN TermMonths BETWEEN 180 AND 191
		THEN 15
		WHEN TermMonths BETWEEN 192 AND 203
		THEN 16
		WHEN TermMonths BETWEEN 204 AND 215
		THEN 17
		WHEN TermMonths BETWEEN 216 AND 227
		THEN 18
		WHEN TermMonths BETWEEN 228 AND 239
		THEN 19
		WHEN TermMonths BETWEEN 240 AND 251
		THEN 20
		WHEN TermMonths BETWEEN 252 AND 263
		THEN 21
		WHEN TermMonths BETWEEN 264 AND 275
		THEN 22
		WHEN TermMonths BETWEEN 276 AND 287
		THEN 23
		WHEN TermMonths BETWEEN 288 AND 299
		THEN 24
		WHEN TermMonths BETWEEN 300 AND 311
		THEN 25
		WHEN TermMonths BETWEEN 312 AND 323
		THEN 26
		WHEN TermMonths BETWEEN 324 AND 335
		THEN 27
		WHEN TermMonths BETWEEN 336 AND 347
		THEN 28
		WHEN TermMonths BETWEEN 348 AND 359
		THEN 29
		WHEN TermMonths BETWEEN 360 AND 371
		THEN 30
		WHEN TermMonths = 600
		THEN 50
		WHEN TermMonths = 1188
		THEN 99
	ELSE NULL
	END AS TermLength,								-- TermLength, DIV1
	0,												-- TermType, Default Value
    0,												-- TermAvailable, Default Value
    CAST(div1.ROYALTY AS NUMERIC(5,4)),				-- RoyaltyAmount, DIV1
    CAST(div1.DelayRental AS INT),					-- DelayRental, DIV1
    CAST(div1.DelayRentalValue AS FLOAT),	        -- RentalAmount, DIV1
    NULL,											-- RentalBonus, Default Value
    NULL,											-- DRVolumePage, Default Value
    NULL,											-- DRRecordNumber, Default Value
    0,  											-- needsRedaction, Default Value
    NULL,											-- ExportedToWeb2, Default Value
    NULL,											-- ExportedToWeb2Date, Default Value
    NULL,											-- ExportedToWeb, Default Value
    NULL,											-- ExportedToWebDate, Default Value
    NULL,											-- FAQDate, Default Value
    NULL,											-- FAQAnsweredDate, Default Value
    NULL,											-- FAQAnsweredBy, Default Value
    NULL,											-- PoorImage, Default Value
    0,												-- NoPriorReference, Default Value
    NULL,											-- MIP, Default Value
    NULL,											-- HandWritten, Default Value
    GETDATE(),										-- _CreatedDateTime, Default Value
    'NA\donald.massey',								-- _CreatedBy, Default Value
    GETDATE(),										-- _ModifiedDateTime, Default Value
    'NA\donald.massey',								-- _ModifiedBy, Default Value
    NULL,											-- causeNumber, Default Value
    NULL,											-- qcBatchID, Default Value
    NULL,											-- auditBatchID, Default Value
    0,												-- IsWebDoc, Default Value
    NULL,											-- SiteId, Default Value
    NULL,											-- Url, Default Value
    1,												-- recordIsLease, Default Value
    0,												-- recordIsCourthouse, Default Value
    0,												-- recordIsAssignment, Default Value
    NULL,											-- prevQCBID, Default Value
    NULL,											-- ParcelNumber, Default Value
    NULL,											-- InstrumentTypeFull, Default Value
    NULL,											-- TotalPages, Default Value
    NULL,											-- Consideration, Default Value
    NULL											-- InstrumentTypeFullId, Default Value
FROM #LND_6732_div1_values div1
LEFT JOIN countyScansTitle.dbo.tblexportlog tel ON tel.LeaseID = div1.LeaseID
LEFT JOIN #LND_6732_cst_values cst ON cst.recordNumber = div1.RecordNumber 
							 AND cst.fileDate = div1.RecordDate
							 AND cst.countyName = div1.CountyName
JOIN countyScansTitle.Tracker.MasterCountyLookup mcl ON mcl.LeasingID = div1.countyID
WHERE tel.LeaseID IS NULL AND mcl.CourthouseTitleID IS NOT NULL;

-- INSERT into tblexportlog for Category 2 records
-- 2,118,373 Records
INSERT INTO countyScansTitle.dbo.tblexportlog (
    recordID,                     -- Gathered from CST.tblrecord
    LeaseID,                      -- Gathered from DIV1
    exportDate,                   -- GETDATE()
    sentToAudit,                  -- Default Value
	zipName,					  -- Default Value
	zipCopied,                    -- Default Value
	imageCopied,                  -- Default Value
    _CreatedDateTime,             -- Handled By Trigger
    _ModifiedDateTime,            -- Handled By Trigger
	StatusOfLastAttempt,          -- Default Value
    DateOfLastAttempt,            -- Default Value
    CountOfLastAttempt            -- Default Value
)
SELECT
    div1.div1_generated_recordid, -- Populated from tblrecord
    div1.LeaseID,                 -- DIV1 LeaseID
    GETDATE(),                    -- Current timestamp as exportDate
    0,                            -- sentToAudit, Default Value
    'LND-6732(2)',				  -- Default Value
	0,                            -- zipCopied, Default Value
	0,                            -- imageCopied, Default Value
	GETDATE(),			          -- _CreatedDateTime, Default Value
	GETDATE(),			          -- _ModifiedDateTime, Default Value
    NULL,                         -- Status of last attempt, Default Value
    NULL,					      -- Current timestamp for DateOfLastAttempt, Default Value
    NULL                          -- Initial count of attempt, Default Value
FROM #LND_6732_div1_values div1
LEFT JOIN countyScansTitle.dbo.tblexportlog tel ON tel.LeaseID = div1.LeaseID
JOIN countyScansTitle.Tracker.MasterCountyLookup mcl ON mcl.LeasingID = div1.countyID
WHERE tel.LeaseID IS NULL;
-- Why didn't these 600 get inserted into tblrecord?


SELECT COUNT(*)
FROM countyScansTitle.dbo.tblrecord
WHERE remarks = 'LND-6732(2)'

SELECT COUNT(*)
FROM countyScansTitle.dbo.tblexportLog
WHERE zipName = 'LND-6732(2)'


-- Rollback the transaction (for testing purposes)
--ROLLBACK TRAN;
-- Uncomment the following line to commit the transaction
COMMIT TRAN;

-- Check transaction count and state
SELECT @@TRANCOUNT, XACT_STATE();

-- 6/3/25 Ales identified that there are/were 638 records in cstitle Dev without countyID, this is likely from records in DIV1 without a county in CST
-- Create a query to identify if this exists going forward and DELETE/MODIFY the records

SELECT tel.*, *
FROM countyScansTitle.dbo.tblrecord tr
LEFT JOIN countyScansTitle.dbo.tblexportLog tel ON tel.recordID = tr.recordID
WHERE countyid IS NULL


-- 1) Ky pulled up the image in Dev using an undeployed version of the App, I inquired on when it could be deployed for testing
-- The inserted record pulls up but the image requires the new App to be deployed which won't happen till the week of 7/28/2025+

-- 2) (Verify only intended records were modified) Returned 49 that weren't processed by the InstrumentTypeEnricher
SELECT CONVERT(varchar, tr._CreatedDateTime, 23), CONVERT(varchar, tr._ModifiedDateTime, 23), *
FROM countyScansTitle.dbo.tblrecord tr
WHERE remarks LIKE '%LND-6732%' 
--AND CONVERT(varchar, tr._CreatedDateTime, 23) != '2025-07-07'
  AND CONVERT(varchar, tr._ModifiedDateTime, 23) != '2025-07-08'

-- 3) (Verify records points to the correct image(s)) The Volume/Page, Record Number for the 7 records processed that day matched in the App
SELECT tel.leaseid, *
FROM countyScansTitle.dbo.tblrecord tr
LEFT JOIN countyScansTitle.dbo.tblexportlog tel ON tel.recordID = tr.recordID
WHERE CONVERT(varchar, tr._ModifiedDateTime, 23) = '2025-07-08'
AND remarks NOT LIKE '%LND-6732%'

-- 4) Verify records aren't produced from land-lease-producer to ES cache
/*
{
  "query": {
    "terms": {
      "recordid": ["00038be6-cba5-4220-8f90-7dc24c6430e4","000a2fc5-faf8-449c-8c92-9f9c81544abd",
                   "001d21d2-0e17-48fb-bb96-078602b771bc","001d6bb5-428f-4a61-9ee8-8018158f53aa",
                   "00216cd5-4125-4e05-b6df-29056450bba2","0022b42c-829e-4b71-8644-b7cc61024f9e",
                   "002c91c5-4e2b-4122-b928-d995b79232ed","0038cee5-ade9-4f0f-a45b-0e5de54d11f3",
                   "003a2d2c-6e65-4f44-a0f1-6e2c0e73cebe","0040afe4-0a12-4d34-9428-ee19363ba8ef"]
    }
  }
}
*/

-- 5) ch-lease-exporter
-- Verify none of the inserted records were produced back into div1_daily.dbo.tblLegalLease
SELECT tr.statusID, *
FROM countyScansTitle.dbo.tblrecord tr
JOIN countyScansTitle.dbo.tblexportLog tel ON tel.recordID = tr.recordID
WHERE tel.LeaseID IN (SELECT LeaseID FROM [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] WHERE created BETWEEN '2025-07-01' AND '2025-07-23')
AND tel.zipName LIKE '%LND-6732%'
-- Can't test in Dev because DIV1 ingestion into implemented

-- 6) land-lease-producer (There haven't been updates since before the inserts but land-lease-producer has been running)
SELECT TOP 10 lease_id, created, updated
FROM [DS9].[pres].[legal_lease]
ORDER BY created DESC

-- 7) ch-database-exports (All modified records are now set to statusID = 90)
SELECT _CreatedBy, _CreatedDateTime, _ModifiedBy, _ModifiedDateTime, AreaAmount, AreaNotAvailable, auditBatchID, bookTypeID, causeNumber, Consideration, countyID, DelayRental, DepthsCoveredFrom, DepthsCoveredTo, DepthsCoveredType, DRRecordNumber, DRVolumePage, effectiveDate, exportDate, exported, ExportedToWeb, ExportedToWeb2, ExportedToWeb2Date, ExportedToWebDate, Extension, ExtensionBLM, ExtensionBonus, ExtensionLength, ExtensionState, ExtensionType, FAQAnsweredBy, FAQAnsweredDate, FAQDate, fileDate, fileExtension, fileType, gathererID, HandWritten, holding, inspectorNumber, instrumentDate, InstrumentTypeFull, instrumentTypeID, IsWebDoc, keyedDate, keyerComments, keyerNumber, LandDescriptionNotAvailable, manualAccept, manualReject, MIP, modifiedDate, MultipleLeasesForTract, needsRedaction, NoPriorReference, originalFileName, outsourcedDate, outsourceID, page, ParcelNumber, pendingDate, PoorImage, prevQCBID, qaReserved, qcBatchID, qcDate, reassignedTo, receivedDate, RecordDate, recordID, recordIsAssignment, recordIsCourthouse, recordIsLease, recordNumber, remarks, RentalAmount, RentalBonus, RoyaltyAmount, SiteId, sourceFilePath, stapled, stapleInspectorNumber, stapleQcDate, stateID, statusID, storageFilePath, supervisorComments, TermAvailable, TermLength, TermType, TotalPages, Url, volume
                             FROM [countyScansTitle].[dbo].[tblrecord] WITH(NOLOCK)
                             WHERE CountyID = {countyid}
                             AND statusID in (4, 10, 16)
                             AND _ModifiedDateTime >= '{first_date}'
                             AND _ModifiedDateTime <= '{last_date}'

-- 8/9) Edit a RecordIsLease recordID in the app and verify the land-lease-producer processes it
-- Modified RecordID's through the GODMODE App to verify changes propagate downstream
SELECT *
FROM countyScansTitle.dbo.tblrecord tr
JOIN countyScansTitle.dbo.tblexportLog tel ON tel.recordID = tr.recordID
WHERE tr.recordID IN ('27ed6fdd-001d-4ead-85c5-8b13f245e720','cde94f11-3562-42f6-8cd9-79b7a6fea24d'
				     ,'0e39d0a7-54ce-4a12-9eb2-badc99192252','3408eb25-1fcc-4e19-a00f-978c9a32b27f'
				     ,'c6f75b56-77dc-42ac-a274-a5410451e786','6982d027-12df-4096-96e6-4b483ac1a300'
				     ,'a54723de-b54b-42ea-b36a-e48047326850','1da20dd5-0316-4d51-b607-b1de973363c7'
					 ,'be3e0cf8-b045-476c-a9a8-bb27e1f560f3','b9e44456-8842-44a1-b4b1-6daaefafd3a6')