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
JOIN countyScansTitle.dbo.tbllookupCounties c ON tr.countyID = c.countyID AND tr.stateID = c.stateID
JOIN countyScansTitle.Tracker.MasterCountyLookup mcl ON mcl.LeasingID = tr.countyID
WHERE tr.InstrumentTypeID IN ('MOGL','OGL','OGLAMD','POGL','OGLEXT','ROGL')
  AND tr.statusID IN (4,10,16,18);


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
JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblCounty] c ON tll.CountyID = c.CountyID
LEFT JOIN [AUS2-DIV1-DDB01].[div1_Daily].[dbo].[tblLegalLeaseDocumentMapping] tlldm ON tll.LeaseID = tlldm.leaseId
LEFT JOIN [AUS2-DIV1-DDB01].[div1_Daily].[dbo].[tblDocument] td ON td.DocumentId = tlldm.documentId
LEFT JOIN [AUS2-DIV1-DDB01].[div1_Daily].[dbo].[tblDocumentFile] tdf ON td.DocumentId = tdf.DocumentId
LEFT JOIN countyScansTitle.Tracker.MasterCountyLookup mcl ON mcl.leasingID = tll.CountyID;


SET XACT_ABORT ON;
BEGIN TRAN;
--/*
-- CATEGORY 1: recordID exists in tblrecord but NOT in tblexportlog
-- 56,157 records
SELECT 
    LOWER(cst.recordID) AS cst_recordID,
    cst.recordNumber AS cst_recordno,
    div1.LeaseID AS div1_leaseid,
    div1.recordNumber AS div1_recordno,
    cst.volume AS cst_volume,
    cst.page AS cst_page,
    div1.volume AS div1_volume,
    div1.page AS div1_page,
    div1.CountyName AS div1_countyName,
    'Category 1: In tblrecord but not in tblexportlog' AS category
FROM #LND_6732_div1_values div1
INNER JOIN #LND_6732_cst_values cst
    ON cst.recordNumber = div1.recordNumber 
    AND cst.fileDate = div1.RecordDate
    AND cst.countyName = div1.CountyName
WHERE NOT EXISTS (
    SELECT 1 
    FROM countyScansTitle.dbo.tblexportLog tel 
    WHERE tel.recordID = cst.recordID
)
 

-- Insert statement for Category 1 records
INSERT INTO countyScansTitle.dbo.tblexportLog (
    recordID, LeaseID, exportDate, sentToAudit, zipName, 
    zipCopied, imageCopied, _CreatedDateTime, _CreatedBy, 
    _ModifiedDateTime, _ModifiedBy, StatusOfLastAttempt, 
    DateOfLastAttempt, CountOfLastAttempt
)
SELECT 
    cst.recordID, 
    div1.LeaseID, 
    GETDATE(), 
    0, 
    'LND-6732(1)', 
    0, 
    0, 
    GETDATE(), 
    'LND-6732(1)', 
    GETDATE(), 
    'LND-6732(1)', 
    NULL, 
    NULL, 
    NULL
FROM #LND_6732_div1_values div1
INNER JOIN #LND_6732_cst_values cst
    ON cst.recordNumber = div1.recordNumber 
    AND cst.fileDate = div1.RecordDate
    AND cst.countyName = div1.CountyName
WHERE NOT EXISTS (
    SELECT 1 
    FROM countyScansTitle.dbo.tblexportLog tel 
    WHERE tel.recordID = cst.recordID
);
--*/

/*
SELECT *
FROM CountyscansTitle.dbo.tblexportLog
WHERE CONVERT(varchar, _CreatedDateTime, 23) = CONVERT(varchar, GETDATE(), 23)
*/

--/*
-- CATEGORY 2: recordid exists in tblrecord and tblexportlog with NULL LeaseID and DIV1 Match
-- 31,811 records
SELECT
    LOWER(cst.recordID) AS cst_recordID,
    cst.recordNumber AS cst_recordno,
    div1.LeaseID AS div1_leaseid,
    div1.recordNumber AS div1_recordno,
    cst.volume AS cst_volume,
    cst.page AS cst_page,
    div1.volume AS div1_volume,
    div1.page AS div1_page,
    div1.CountyName AS div1_countyName,
	'Category 2: Exists in countyScansTitle with NULL LeaseID' AS category
FROM countyScansTitle.dbo.tblexportLog tel
INNER JOIN #LND_6732_cst_values cst 
    ON tel.recordID = cst.recordID
INNER JOIN #LND_6732_div1_values div1 
    ON cst.recordNumber = div1.recordNumber 
    AND cst.fileDate = div1.RecordDate
    AND cst.countyName = div1.CountyName
WHERE tel.LeaseID IS NULL;

-- Update statement for Category 2 records
UPDATE tel
SET 
    tel.LeaseID = div1.LeaseID,
    tel._ModifiedDateTime = GETDATE()
FROM countyScansTitle.dbo.tblexportLog tel
INNER JOIN #LND_6732_cst_values cst 
    ON tel.recordID = cst.recordID
INNER JOIN #LND_6732_div1_values div1 
    ON cst.recordNumber = div1.recordNumber 
    AND cst.fileDate = div1.RecordDate
    AND cst.countyName = div1.CountyName
WHERE tel.LeaseID IS NULL;
--*/


/*
SELECT *
FROM CountyscansTitle.dbo.tblexportLog
WHERE CONVERT(varchar, _ModifiedDateTime, 23) = CONVERT(varchar, GETDATE(), 23)
*/

--/*
-- CATEGORY 3: Records in Div1 that don't have a match in CST (doesn't exist in both tblexportlog and tblrecord)
-- 3,759,990 Records
SELECT 
    cst.recordID AS cst_recordID,
    cst.recordNumber AS cst_recordno,
    div1.LeaseID AS div1_leaseid,
	div1_generated_recordid,
    div1.recordNumber AS div1_recordno,
    cst.volume AS cst_volume,
    cst.page AS cst_page,
    div1.volume AS div1_volume,
    div1.page AS div1_page,
    div1.CountyName AS div1_countyName,
    'Category 3: Exists in Div1 but not in tblrecord or tblexportlog' AS category
FROM #LND_6732_div1_values div1
LEFT JOIN #LND_6732_cst_values cst ON 
    cst.recordNumber = div1.recordNumber 
    AND cst.fileDate = div1.RecordDate
    AND cst.countyName = div1.CountyName
LEFT JOIN countyScansTitle.dbo.tblexportLog tel ON tel.LeaseID = div1.LeaseID
WHERE cst.recordID IS NULL
  AND tel.LeaseID IS NULL;


-- Create New Status In tblStatus
INSERT INTO countyScansTitle.dbo.tblStatus
(statusID, stapleDescription, dataEntryDescription, IncomingInspectionDescription, assignmentDescription)
SELECT
	90,								-- statusID
	NULL,							-- stapleDescription
	'Leases Mapped From DIV1',		-- dataEntryDescription
	'not currently in use',			-- IncomingInspectionDescription
	'not currently in use'			-- assignmentDescription


-- INSERT into tblrecord for Category 3 records
-- Default Value (Gathered from Ryan M Mapping / csdigital-to-cstitle)
-- 3,759,990 Records
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
    'LND-6732(3)',									-- remarks, DIV1
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
		WHEN div1.TermMonths IS NULL
		THEN NULL
		WHEN div1.TermMonths = 0
		THEN 0
		WHEN div1.TermMonths = 12
		THEN 1
		WHEN div1.TermMonths = 24
		THEN 2
		WHEN div1.TermMonths = 36
		THEN 3
		WHEN div1.TermMonths = 48
		THEN 4
		WHEN div1.TermMonths = 60
		THEN 5
		WHEN div1.TermMonths = 72
		THEN 6
		WHEN div1.TermMonths = 84
		THEN 7
		WHEN div1.TermMonths = 96
		THEN 8
		WHEN div1.TermMonths = 108
		THEN 9
		WHEN div1.TermMonths = 120
		THEN 10
		WHEN div1.TermMonths = 132
		THEN 11
		WHEN div1.TermMonths = 144
		THEN 12
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
    'NA\donald.massey',									-- _CreatedBy, Default Value
    GETDATE(),										-- _ModifiedDateTime, Default Value
    'NA\donald.massey',									-- _ModifiedBy, Default Value
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
LEFT JOIN #LND_6732_cst_values cst ON 
    cst.recordNumber = div1.RecordNumber 
    AND cst.fileDate = div1.RecordDate
    AND cst.countyName = div1.CountyName
LEFT JOIN countyScansTitle.dbo.tblexportlog tel ON tel.LeaseID = div1.LeaseID
LEFT JOIN [countyScansTitle].[Tracker].[MasterCountyLookup] mcl ON div1.countyID = mcl.LeasingID
WHERE cst.recordID IS NULL
    AND tel.LeaseID IS NULL;


-- INSERT into tblexportlog for Category 3 records
-- 3,802,980 Records
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
    'LND-6732(3)',				  -- Default Value
	0,                            -- zipCopied, Default Value
	0,                            -- imageCopied, Default Value
	GETDATE(),			          -- _CreatedDateTime, Default Value
	GETDATE(),			          -- _ModifiedDateTime, Default Value
    NULL,                         -- Status of last attempt, Default Value
    NULL,					      -- Current timestamp for DateOfLastAttempt, Default Value
    NULL                          -- Initial count of attempt, Default Value
FROM #LND_6732_div1_values div1
LEFT JOIN [countyScansTitle].[Tracker].[MasterCountyLookup] mcl ON div1.countyID = mcl.LeasingID
LEFT JOIN countyScansTitle.dbo.tblexportlog tel ON tel.LeaseID = div1.LeaseID
WHERE tel.LeaseID IS NULL;
--*/

/*
SELECT *
FROM countyScansTitle.dbo.tblrecord
WHERE CONVERT(varchar, _CreatedDateTime, 23) = CONVERT(varchar, GETDATE(), 23)

SELECT *
FROM countyScansTitle.dbo.tblexportLog
WHERE _CreatedBy = 'na\donald.massey' AND CONVERT(varchar, _CreatedDateTime, 23) = CONVERT(varchar, GETDATE(), 23)
*/


-- SUMMARY COUNTS
SELECT 'Category 1: In tblrecord but not in tblexportlog' AS Category,
       COUNT(*) AS RecordCount
FROM #LND_6732_div1_values div1
INNER JOIN #LND_6732_cst_values cst
    ON cst.recordNumber = div1.recordNumber 
    AND cst.fileDate = div1.RecordDate
    AND cst.countyName = div1.CountyName
WHERE NOT EXISTS (
    SELECT 1 
    FROM countyScansTitle.dbo.tblexportLog tel 
    WHERE tel.recordID = cst.recordID
)

UNION ALL

SELECT 'Category 2: Exists in both tblexportlog and tblrecord' AS Category,
       COUNT(*) AS RecordCount
FROM #LND_6732_div1_values div1
INNER JOIN #LND_6732_cst_values cst
    ON cst.recordNumber = div1.recordNumber 
    AND cst.fileDate = div1.RecordDate
    AND cst.countyName = div1.CountyName
INNER JOIN countyScansTitle.dbo.tblexportLog tel 
    ON tel.recordID = cst.recordID
WHERE tel.LeaseID IS NULL

UNION ALL

SELECT 'Category 3: Exists in Div1 but not in tblrecord or tblexportlog' AS Category,
       COUNT(*) AS RecordCount
FROM #LND_6732_div1_values div1
LEFT JOIN countyScansTitle.dbo.tblexportLog tel 
    ON tel.LeaseID = div1.LeaseID
LEFT JOIN #LND_6732_cst_values cst
    ON cst.recordNumber = div1.recordNumber 
    AND cst.fileDate = div1.RecordDate
    AND cst.countyName = div1.CountyName
WHERE tel.LeaseID IS NULL;


-- Rollback the transaction (for testing purposes)
--ROLLBACK TRAN;
-- Uncomment the following line to commit the transaction
COMMIT TRAN;

-- Check transaction count and state
SELECT @@TRANCOUNT, XACT_STATE();