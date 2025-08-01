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
FROM [linktoDiv1Repl].[div1_daily].[dbo].[tblLegalLease] tll
LEFT JOIN [linktoDiv1Repl].[div1_daily].[dbo].[tblCounty] c ON c.CountyID = tll.CountyID
LEFT JOIN [linktoDiv1Repl].[div1_Daily].[dbo].[tblLegalLeaseDocumentMapping] tlldm ON tlldm.leaseId = tll.LeaseID
LEFT JOIN [linktoDiv1Repl].[div1_Daily].[dbo].[tblDocument] td ON td.DocumentId = tlldm.documentId
LEFT JOIN [linktoDiv1Repl].[div1_Daily].[dbo].[tblDocumentFile] tdf ON tdf.DocumentId = td.DocumentId
LEFT JOIN countyScansTitle.Tracker.MasterCountyLookup mcl ON mcl.leasingID = tll.CountyID;

-- 3,058,774
SELECT COUNT(*)
FROM #LND_6732_div1_values

/*
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
*/


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
WHERE tel.LeaseID IS NULL AND mcl.CourthouseTitleID IS NOT NULL
-- GROUP 1
--AND mcl.CourthouseTitleID IN (220,126,360,590,299,538,595,411,654,623,607,624,621,579,577,586);

-- GROUP 2
AND mcl.CourthouseTitleID IN (673,604,622,268,534,628,541,661,418,633,582,619,531,57,574,665,61,210,614,652,529,618,589,201,290,410,83,530,174,331,605,524,585,102,575,302,430,664,630,606,143,587,212,655,128,600,572,666,578,1,629);

-- GROUP 3
--AND mcl.CourthouseTitleID IN (629,576,543,536,482,451,556,584,581,610,330,647,528,75,37,608,335,642,525,588,565,571,229,303,526,21,329,662,251,416,304,559,548,542,223,409,388,551,184,403,108,62,450,58,641,616,203,609,353,183,146,679,425,123,602,591,407,376,518,445,676,107,139,187,613,519,412,549,599,372,81,657,653,656,615,603,144,428,241,364,214,321,429,91,315,437,393,439,313,339,308,631,383,562,45,573,1031,89,230,121,452,674,20,449,568,487,379,155,375,651,566,257,36,537);

-- GROUP 4
--AND mcl.CourthouseTitleID IN (434,446,7,324,249,535,668,297,363,637,253,64,380,408,111,667,617,398,149,76,494,649,239,497,3,109,367,153,427,520,152,512,385,359,507,414,334,348,436,484,110,458,79,381,400,242,502,369,421,101,422,660,158,88,650,593,496,92,390,1185,404,545,567,527,583,672,13,392,433,522,106,503,485,569,470,343,620,235,175,391,395,208,66,147,597,177,420,240,72,351,148,361,598,354,580,228,397,532,328,179,8,596,234,521,678,120,594,370,17,49,533,168,127,215,342,442,40,394,970,263,382,643,481,336,406,435,247,465,473,345,675,221,468,471,489,564,317,182,252,162,204,447,601,681,443,84,312,498,132,39,516,99,213,611,98,511,455,356,488,95,200,346,634,320,250,333,181,202,197,373,237,82,626,677,178,100,592,217,245,501,365,55,419,340,625,205,125,104,509,29,636,505,612,475,176,224,555,454,670,658,438,166,327,472,161,357,561,456,347,457,499,85,413,366,424,638,310,352,68,483,396,474,5,389,659,18,648,459,645,124,523,103,492,842,51,417,552,24,680,646,378,67,683,196,639,682,137,460,440,510,506,550,570,254,448,966,820,371,314,97,386,170,341,384,355,349,311,26,560,362,423,453,77,350,71,30,426,663,644,415,332,207,463,368,195,441,490,216,802,513,476,478,495,222,4,973,309,138,209,627,402,48,431,141,671,405,563,467,480,546,558,962,243,769,374,669,358,1186,965,967,322,69,517,41,783,544,477,387,461,325,810,491,117,554,275,963,180,47,319,265,812,466,233,795,888,777,972,800,760,640,765,432,135,758,775,557,493,173,87,539,635,540,807,969,326,151,940,964,131,277,737,742,779,337,23,752,766,1050,790,305,759,1042,318,632,73,787,755,763,745,188,799,226,772,942,90,113,896,764,789,159,338,731,951,780,968,218,943,744,500,316,50,279,198,748,464,266,10,762,936,824,278,444,1054,38,944,1103,1044,399,844,768,22,504,818,793,952,469,822,961,798,805,547,276,804,273,272,283,122,462,274,845,729,145,829,727,515,256,186,553,773,1077,874,53,193,189,953,344,259,301,782,286,307,939,271,828,280,803,156,933,2,735,377,93,1047,848,261,136,260,154,784,508,479,486,948,771,741,949,946,14,1101,1160,133,114,236,231,813,788,725,923,267,761,238,165,1079,1129,956,287,955,749,1091,262,1093,841,801,1084,1043,80,1110,718,753,739,285,192,751,514,954,1152,827,934,959,1109,1099,1183,734,1065,118,52,248,732,750,816,825,1165,730,855,746,255,323,281,830,838,1181,1046,911,864,878,747,282,941,1184,1167,817,726,264,937,74,56,776,957,1049,1009,1026,1036,1175,1135,857,1015,1082,1087,306,1006,947,861,862,721,134,834,1094,1041,976,991,994,1002,1007,723);



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
WHERE tel.LeaseID IS NULL AND mcl.CourthouseSubID IS NOT NULL

-- GROUP 1
--AND mcl.CourthouseTitleID IN (220,126,360,590,299,538,595,411,654,623,607,624,621,579,577,586);

-- GROUP 2
AND mcl.CourthouseTitleID IN (673,604,622,268,534,628,541,661,418,633,582,619,531,57,574,665,61,210,614,652,529,618,589,201,290,410,83,530,174,331,605,524,585,102,575,302,430,664,630,606,143,587,212,655,128,600,572,666,578,1,629);

-- GROUP 3
--AND mcl.CourthouseTitleID IN (629,576,543,536,482,451,556,584,581,610,330,647,528,75,37,608,335,642,525,588,565,571,229,303,526,21,329,662,251,416,304,559,548,542,223,409,388,551,184,403,108,62,450,58,641,616,203,609,353,183,146,679,425,123,602,591,407,376,518,445,676,107,139,187,613,519,412,549,599,372,81,657,653,656,615,603,144,428,241,364,214,321,429,91,315,437,393,439,313,339,308,631,383,562,45,573,1031,89,230,121,452,674,20,449,568,487,379,155,375,651,566,257,36,537);

-- GROUP 4
--AND mcl.CourthouseTitleID IN (434,446,7,324,249,535,668,297,363,637,253,64,380,408,111,667,617,398,149,76,494,649,239,497,3,109,367,153,427,520,152,512,385,359,507,414,334,348,436,484,110,458,79,381,400,242,502,369,421,101,422,660,158,88,650,593,496,92,390,1185,404,545,567,527,583,672,13,392,433,522,106,503,485,569,470,343,620,235,175,391,395,208,66,147,597,177,420,240,72,351,148,361,598,354,580,228,397,532,328,179,8,596,234,521,678,120,594,370,17,49,533,168,127,215,342,442,40,394,970,263,382,643,481,336,406,435,247,465,473,345,675,221,468,471,489,564,317,182,252,162,204,447,601,681,443,84,312,498,132,39,516,99,213,611,98,511,455,356,488,95,200,346,634,320,250,333,181,202,197,373,237,82,626,677,178,100,592,217,245,501,365,55,419,340,625,205,125,104,509,29,636,505,612,475,176,224,555,454,670,658,438,166,327,472,161,357,561,456,347,457,499,85,413,366,424,638,310,352,68,483,396,474,5,389,659,18,648,459,645,124,523,103,492,842,51,417,552,24,680,646,378,67,683,196,639,682,137,460,440,510,506,550,570,254,448,966,820,371,314,97,386,170,341,384,355,349,311,26,560,362,423,453,77,350,71,30,426,663,644,415,332,207,463,368,195,441,490,216,802,513,476,478,495,222,4,973,309,138,209,627,402,48,431,141,671,405,563,467,480,546,558,962,243,769,374,669,358,1186,965,967,322,69,517,41,783,544,477,387,461,325,810,491,117,554,275,963,180,47,319,265,812,466,233,795,888,777,972,800,760,640,765,432,135,758,775,557,493,173,87,539,635,540,807,969,326,151,940,964,131,277,737,742,779,337,23,752,766,1050,790,305,759,1042,318,632,73,787,755,763,745,188,799,226,772,942,90,113,896,764,789,159,338,731,951,780,968,218,943,744,500,316,50,279,198,748,464,266,10,762,936,824,278,444,1054,38,944,1103,1044,399,844,768,22,504,818,793,952,469,822,961,798,805,547,276,804,273,272,283,122,462,274,845,729,145,829,727,515,256,186,553,773,1077,874,53,193,189,953,344,259,301,782,286,307,939,271,828,280,803,156,933,2,735,377,93,1047,848,261,136,260,154,784,508,479,486,948,771,741,949,946,14,1101,1160,133,114,236,231,813,788,725,923,267,761,238,165,1079,1129,956,287,955,749,1091,262,1093,841,801,1084,1043,80,1110,718,753,739,285,192,751,514,954,1152,827,934,959,1109,1099,1183,734,1065,118,52,248,732,750,816,825,1165,730,855,746,255,323,281,830,838,1181,1046,911,864,878,747,282,941,1184,1167,817,726,264,937,74,56,776,957,1049,1009,1026,1036,1175,1135,857,1015,1082,1087,306,1006,947,861,862,721,134,834,1094,1041,976,991,994,1002,1007,723);



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
WHERE tel.LeaseID IN (SELECT LeaseID FROM [linktoDiv1Repl].[div1_daily].[dbo].[tblLegalLease] WHERE created BETWEEN '2025-07-01' AND '2025-07-23')
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