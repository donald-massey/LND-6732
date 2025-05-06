SELECT TOP 1 *
FROM [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease]

SELECT TOP 1 *
FROM [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLeaseDocumentMapping]

SELECT TOP 1 *
FROM [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblleaseAbstractMapping]
WHERE descriptions IS NOT NULL AND descriptions != ''

SELECT TOP 1 *
FROM [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblAbstract]


-- Used values from this query to map back to tblLandDescription, verify any issues with Ty
SELECT TOP 10 *
FROM [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] tll
LEFT JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLeaseDocumentMapping] tlldm ON tlldm.leaseid = tll.leaseid
LEFT JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblleaseAbstractMapping] tlam ON tlam.mappingid = tlldm.legalleasedocumentmappingid
LEFT JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblAbstract] ta ON ta.abstractid = tlam.abstractid
LEFT JOIN countyScansTitle.dbo.tblExportLog tel ON tll.leaseid = tel.LeaseID
WHERE ta.AbstractID IS NOT NULL
--AND tlam.parcelNum IS NOT NULL
--AND (tlam.sectiondetail IS NOT NULL AND (tlam.descriptions IS NOT NULL AND tlam.descriptions != ''))
AND tll.leaseid = '1770716';


SELECT TOP 100 *
FROM countyScansTitle.dbo.tbllandDescription


-- How to split tlam.descriptions into QuarterCalls (N/4, S/4, etc) and BriefLegal (LOT ##-## SEC ## BLK ##)
SELECT TOP 5000 LOWER(NEWID()) AS landDescriptionID
			 ,LOWER(tr.recordID) AS recordID  -- From tblRecord
			 ,tr.countyID AS CountyID  -- From tblRecord
			 ,ta.ohSubDivisionName AS Subdivision  -- All of these were NULL
			 ,ta.surveyName AS Survey  -- From tblAbstract
			 ,CAST(ta.ohLot AS INT) AS Lot  -- All of these were NULL
			 ,ta.SurveyBlock AS Block  -- From tblAbstract
			 ,CAST(ta.Section AS INT) AS Section  -- From tblAbstract
			 ,CAST(ta.Township AS INT) AS Township  -- From tblAbstract
			 ,COALESCE(CONCAT(CAST(ta.Range AS INT), ta.RangeDirection), ta.SurveyBlock) AS RangeOrBlock -- Values from TOP 1000 are A## - A##?  -- From tblAbstract
			 ,NULL AS AbstractName  -- This is a reference to the Abstract Number From DIV1 not the AbstractID
			 ,NULL AS Suffix  -- Default Value
			 ,NULL AS QuarterCalls  -- Skip For Now
			 ,NULL AS AcreageByTract  -- tll.acres
			 ,NULL AS TractDRRecordNumber  -- No values
			 ,NULL AS TractDRVolumePage  -- No values
			 ,NULL AS PlatVolumePage  -- No values
			 ,NULL AS PlatRecordNumber  -- No Values
			 ,tlam.descriptions AS BriefLegal -- tlam.Descriptions, needs a case statement
			 ,NULL AS PatentNumber -- 6 Values
			 ,NULL AS PatentVolume  -- 6 Values
			 ,NULL AS CertificateNumber  -- Default Values
			 ,NULL AS FileNumber  -- Default Values
			 ,NULL AS PlatVolumeCabinet  -- Default Values
			 ,NULL AS PlatPageSlide  -- Default Values
			 ,NULL AS PlatBookType  -- Default Values
			 ,NULL AS QuarterCallsFull  -- Import all quartercalls
			 ,NULL AS NewCityBlock  -- Default Values
			 ,NULL AS SubdivisionNameId  -- Default Values
			 ,0 AS IsDeleted  -- Default Value
FROM [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] tll
LEFT JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLeaseDocumentMapping] tlldm ON tlldm.leaseid = tll.leaseid
LEFT JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblleaseAbstractMapping] tlam ON tlam.mappingid = tlldm.legalleasedocumentmappingid
LEFT JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblAbstract] ta ON ta.abstractid = tlam.abstractid
LEFT JOIN countyScansTitle.dbo.tblExportLog tel ON tll.leaseid = tel.LeaseID
LEFT JOIN countyScansTitle.dbo.tblrecord tr ON tel.recordID = tr.recordID
WHERE COALESCE(CONCAT(CAST(ta.Range AS INT), ta.RangeDirection), ta.SurveyBlock) LIKE '%a%';
-- BriefLegal vs quarterCalls?


SELECT TOP 100 tld.*, tel.*
FROM countyScansTitle.dbo.tblrecord tr
LEFT JOIN countyscanstitle.dbo.tblexportlog tel ON tr.recordid = tel.recordid
LEFT JOIN countyScansTitle.dbo.tbllandDescription tld ON tr.recordID = tld.recordID
WHERE tel.zipname LIKE ('%mapping%')
AND QuarterCallsFull IS NOT NULL


-- Process them into buckets and then apply transformation logic to the buckets
-- Should the SUBDIVISION values be used?
-- Should the SEE * values be used?
-- Should the ALL values be used?
-- Should the FRACTIONAL values be used?
-- Should the FRAC values be used?
-- Should the PORT values be used?
-- Should the A values be used?
-- Should the ALL OF SEC or SEC. values be used?
-- Should the 160a values be used?
-- Should the 80a values be used?
-- Should the 40a values be used?
-- Should the 240ac / 80ac / 20ac / 168.50ac?

-- Create a bucket for TRACTS
SELECT
    tlam.descriptions,
    CASE  -- Creating logical groupings to filter down
        WHEN tlam.descriptions LIKE '%;%' OR tlam.descriptions LIKE '%,%' OR tlam.descriptions LIKE '% %' OR tlam.descriptions LIKE '%/%' OR tlam.descriptions LIKE '%+%' AND (tlam.descriptions NOT LIKE '%lot%' OR tlam.descriptions NOT LIKE '%tract%' OR tlam.descriptions NOT LIKE '%subdivision%') THEN 'Contains Semicolon, Comma, Space, Slash or Plus symbol'
		WHEN tlam.descriptions LIKE '%tract' AND (tlam.descriptions NOT LIKE '%;%' OR tlam.descriptions NOT LIKE '%,%' OR tlam.descriptions NOT LIKE '% %' OR tlam.descriptions NOT LIKE '%/%' OR tlam.descriptions NOT LIKE '%+%' AND tlam.descriptions NOT LIKE '%lot%') THEN 'Contains TRACT'
        WHEN tlam.descriptions LIKE '%lot%' AND (tlam.descriptions LIKE '%&%' OR tlam.descriptions LIKE '% %' OR tlam.descriptions LIKE '%,%') THEN 'Contains Lot'
        WHEN tlam.descriptions LIKE '%&%'   AND (tlam.descriptions NOT LIKE '%lot%' OR tlam.descriptions NOT LIKE '% %') THEN 'Contains Ampersand'
        WHEN tlam.descriptions LIKE '%all%'  THEN 'Contains all'
        WHEN tlam.descriptions LIKE '%frac%' THEN 'Contains frac'
        WHEN tlam.descriptions = 'SEE BELOW' THEN 'Contains SEE BELOW'
        WHEN tlam.descriptions LIKE '%subdivision%' THEN 'Contains subdivision'
        WHEN tlam.descriptions LIKE '%0a%' OR tlam.descriptions LIKE '%ac%' THEN 'Contains 0a or ac'
        WHEN tlam.descriptions LIKE '%port%' AND tlam.descriptions NOT LIKE '% %' AND tlam.descriptions NOT LIKE '%/%' THEN 'Contains port'
        ELSE 'Does Not Contain Comma, Semicolon, Ampersand, Plus, lot, Space or Slash, 0a or ac, frac, all, or subdivision'
    END AS GroupType
FROM [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] tll
LEFT JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLeaseDocumentMapping] tlldm ON tlldm.leaseid = tll.leaseid
LEFT JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblleaseAbstractMapping] tlam ON tlam.mappingid = tlldm.legalleasedocumentmappingid
LEFT JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblAbstract] ta ON ta.abstractid = tlam.abstractid
LEFT JOIN countyScansTitle.dbo.tblExportLog tel ON tll.leaseid = tel.LeaseID
LEFT JOIN countyScansTitle.dbo.tblrecord tr ON tel.recordID = tr.recordID
WHERE (ta.Section IS NOT NULL AND NULLIF(tlam.descriptions, '') IS NOT NULL)
  AND (
       -- Matching Semicolon or Comma without 'lot'
       (tlam.descriptions LIKE '%;%' OR tlam.descriptions LIKE '%,%') AND tlam.descriptions NOT LIKE '%lot%'
       -- Matching 'lot' with additional conditions
       OR (tlam.descriptions LIKE '%lot%' AND (tlam.descriptions LIKE '%&%' OR tlam.descriptions LIKE '% %' OR tlam.descriptions LIKE '%,%'))
       -- Matching Ampersand without 'lot' or Space
       OR (tlam.descriptions LIKE '%&%' AND (tlam.descriptions NOT LIKE '%lot%' OR tlam.descriptions NOT LIKE '% %'))
       -- Matching Plus
       OR tlam.descriptions LIKE '%+%'
       -- Matching 'frac'
       OR tlam.descriptions LIKE '%frac%'
       -- Matching 'all'
       OR tlam.descriptions LIKE '%all%'
       -- Matching 'SEE BELOW'
       OR tlam.descriptions = 'SEE BELOW'
       -- Matching 'subdivision'
       OR tlam.descriptions LIKE '%subdivision%'
       -- Matching '0a' or 'ac'
       OR tlam.descriptions LIKE '%0a%' OR tlam.descriptions LIKE '%ac%'
       -- Matching 'port' with restrictions
       OR (tlam.descriptions LIKE '%port%' AND tlam.descriptions NOT LIKE '% %' AND tlam.descriptions NOT LIKE '%/%')
       -- Matching Space or Slash
       OR tlam.descriptions LIKE '% %' OR tlam.descriptions LIKE '%/%');


-- Filter QuarterCallsFull down to Contains Space or Slash, Contains Semicolon or Comma
-- After creating the filter, verify there aren't edge cases included