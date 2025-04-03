/* The following SQL queries provide different approaches to evaluate the quality of record matches between the County Scans Title (CST) and Div 1 systems.

	1. Match Distribution Analysis
	Analyzes the distribution of matches across different match types and counties. 
	Reveals if certain match types are disproportionately represented and identifies county-specific data quality patterns.

	2. Holistic Quality Report
	Creates a comprehensive quality report summarizing multiple metrics: match type distribution, multiple match issues, and confidence score distribution. 
	Provides an executive summary of the overall matching quality.

	3. Match Criteria Overlap
	Identifies records matched by multiple criteria (exact number, volume-record number, substring) to detect redundancies in the matching logic or potential false positives. 
	Helpful for determining if certain match rules are unnecessary.

	4. Multiple Map Detection
	Detects one-to-many and many-to-one relationships between CST and DIV1 records. 
	Shows CST records matching multiple DIV1 records and vice versa, which may indicate data quality issues or overly permissive matching criteria.

	5. Match Confidence Scoring
	Assigns a numerical confidence score (40-160) to each match based on match type and attribute similarity. 
	Helps prioritize high-confidence matches and flag low-confidence matches for additional verification.

	6. Potential Missed Matches
	Searches for records with similar characteristics that weren't caught by current matching criteria. 
	Helps identify potential gaps in matching logic by finding records with comparable dates and similar, but not matching, record numbers.

	7. Edge Case Detection
	Identifies potential false positives in substring matching by calculating what percentage of the longest record number string is matched. 
	Flags matches below 70% similarity for additional scrutiny.

	8. Sample For Manual Review
	Generates stratified random samples of 20 records from each match type (exact, volume-record, substring) for manual verification by domain experts. 
	Essential for validating algorithmic matches against human judgment. */


/* Execution Instructions

	Setting Up Temporary Tables
		To create the necessary temporary tables in SQL Server, please select and execute:
		   - **Lines 95 through 215**
		   - In SQL Server Management Studio (SSMS), highlight these lines and press F5 or click the "Execute" button

	Running QC Queries
		After the temporary tables are successfully created:
		   - The QC (Quality Control) queries begin at **Line 208**
		   - Highlight and execute these queries individually or as needed */


/* CountyScansTitle Temporary Table Documentation:
   This query extracts and transforms record data into temporary table #LND_6732_modded_cst_values with the following column modifications:

• recordID
  - Direct selection of the unique record identifier from tblrecord
  - No transformation applied
• recordNumber
  - Direct selection of the original record number as stored in the database
  - No transformation applied
• bigint_recordNumber
  - Extracts only numeric portions from recordNumber
  - Uses TRANSLATE and REPLACE to remove all alphabetic characters and hyphens
  - Converts result to BIGINT data type
  - Returns NULL if resulting numeric value equals 0
  - Purpose: Standardizes numeric record identifiers for sorting and indexing
• nvarchar_recordNumber
  - Extracts only numeric portions from recordNumber
  - Uses TRANSLATE and REPLACE to remove all alphabetic characters and hyphens
  - Preserves as NVARCHAR data type
  - Purpose: Allows string operations on cleaned numeric portions
• fileDate
  - Converts original fileDate to yyyy-mm-dd format string (varchar)
  - Purpose: Standardizes date format for consistency
• volume
  - Attempts to convert volume field to INTEGER
  - Returns NULL for empty strings or non-numeric values
  - Returns NULL if numeric value equals 0
  - Purpose: Creates clean numeric volume values for sorting/filtering
• page
  - Attempts to convert page field to INTEGER
  - Returns NULL for empty strings or non-numeric values
  - Returns NULL if numeric value equals 0
  - Purpose: Creates clean numeric page values for sorting/filtering
• countyID
  - Direct selection of county identifier from tblrecord
  - No transformation applied

Values Gathered:
	• Records filtered for Texas (stateID = 48) with specific counties or New Mexico (stateID = 35) county Eddy
	• Only records where LeaseID is NULL in the export log (not previously exported)
	• Only oil and gas lease related instruments (MOGL, OGL, OGLAMD, POGL, OGLEXT, ROGL)
	• Only records with statusIDs: 4, 10, 16, 18 (representing specific record statuses) */

-- Check if temporary table exists and drop it if it does
IF OBJECT_ID('tempdb..#LND_6732_cst_values', 'U') IS NOT NULL
    DROP TABLE #LND_6732_cst_values;

SELECT tr.recordID as recordID
      ,recordNumber AS recordNumber
	  ,CASE
			WHEN TRY_CONVERT(INT, ISNULL(tr.volume,'')) = 0
			THEN NULL
			ELSE TRY_CONVERT(INT, ISNULL(tr.volume,''))
	   END AS volume
	  ,CASE
			WHEN TRY_CONVERT(INT, ISNULL(tr.page,'')) = 0
			THEN NULL
			ELSE TRY_CONVERT(INT, ISNULL(tr.page,''))
	   END AS page
      ,CONVERT(varchar, tr.fileDate, 23) AS fileDate
      ,tlc.CountyName
INTO #LND_6732_cst_values
FROM countyScansTitle.dbo.tblrecord tr
LEFT JOIN countyScansTitle.dbo.tblexportLog tel ON tr.recordID = tel.recordID
LEFT JOIN countyScansTitle.dbo.tbllookupCounties tlc ON tr.countyID = tlc.countyID
WHERE (tr.stateID = 48 
   AND tr.countyID IN (2,26,52,53,64,87,93,113,114,118,145,151,154,156,165,186,192,195,198,231,236,238,248)
    OR tr.stateID = 35 AND tr.countyID IN (263))
  AND tr.InstrumentTypeID IN ('MOGL','OGL','OGLAMD','POGL','OGLEXT','ROGL')
  AND statusID IN (4,10,16,18);


/* DIV1 Temporary Table Documentation:
	This query extracts and transforms record data into temporary table #LND_6732_modded_div1_values with the following column modifications:

• LeaseID
  - Direct selection of the lease identifier from tblLegalLease
  - No transformation applied
• recordNumber
  - Direct selection of the original record number as stored in the database
  - No transformation applied
• bigint_recordNumber
  - Extracts only numeric portions from recordNumber
  - Uses TRANSLATE and REPLACE to remove all alphabetic characters and hyphens
  - Converts result to BIGINT data type
  - Returns NULL if resulting numeric value equals 0
  - Purpose: Standardizes numeric record identifiers for sorting and indexing
• nvarchar_recordNumber
  - Extracts only numeric portions from recordNumber
  - Uses TRANSLATE and REPLACE to remove all alphabetic characters and hyphens
  - Preserves as NVARCHAR data type
  - Purpose: Allows string operations on cleaned numeric portions
• RecordDate
  - Converts original RecordDate to yyyy-mm-dd format string (varchar)
  - Purpose: Standardizes date format for consistency
• volume
  - Attempts to convert Vol field to INTEGER
  - Returns NULL if numeric value equals 0
  - Purpose: Creates clean numeric volume values for sorting/filtering
• page
  - Attempts to convert pg field to INTEGER
  - Returns NULL if numeric value equals 0
  - Purpose: Creates clean numeric page values for sorting/filtering

Values Gathered:
	• Records filtered for specific counties (24 total) including:
	  - 23 counties from Texas (including Andrews, Burleson, Reagan, Reeves, etc.)
	  - 1 county from New Mexico (Eddy, NM - CountyID 408)
	• Data is sourced from the Div 1 database (AUS2-DIV1-DDB01.div1_daily)
	• No additional filtering criteria applied beyond county selection */

-- Check if temporary table exists and drop it if it does
IF OBJECT_ID('tempdb..#LND_6732_div1_values', 'U') IS NOT NULL
    DROP TABLE #LND_6732_div1_values;

SELECT LeaseID
      ,recordNumber
      ,CONVERT(varchar, RecordDate, 23) as RecordDate
	  ,CASE
			WHEN TRY_CONVERT(INT, ISNULL(tll.Vol,'')) = 0
			THEN NULL
			ELSE TRY_CONVERT(INT, ISNULL(tll.Vol,''))
	   END AS volume
	  ,CASE
			WHEN TRY_CONVERT(INT, ISNULL(tll.pg,'')) = 0
			THEN NULL
			ELSE TRY_CONVERT(INT, ISNULL(tll.pg,''))
	   END AS page
      ,tc.CountyName
INTO #LND_6732_div1_values
FROM [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] tll
LEFT JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblCounty] tc ON tc.CountyID = tll.CountyID
WHERE tll.CountyID IN (2,26,52,53,64,86,91,111,112,116,143,149,152,154,163,184,190,193,196,229,234,236,246,408);
-- End Temporary Table Creation


-- Begin QC Queries
/* MATCH DISTRIBUTION ANALYSIS
   --------------------------
 
  PURPOSE:
  This query analyzes the distribution of matches across different match types and counties.
  Understanding these distributions helps:
    1. Identify if certain match types are disproportionately represented
    2. Detect county-specific data quality issues or patterns
    3. Validate that the match distribution aligns with expectations
 
  HOW IT WORKS:
  1. The query creates a common table expression (CTE) called 'match_results' containing
     three UNION ALL sections (one for each match type) that identify all matched pairs
     while tagging each with its match type and preserving the county information
 
  2. The first part of the main query then:
     - Groups by match_type
     - Counts matches for each type
     - Calculates what percentage of total matches each type represents
     - Orders results by count (highest first)
 
  3. The second part:
     - Joins to the tblcounty table to get county names
     - Groups by county name and match type
     - Counts matches for each county+type combination
     - Orders by county name and then by count
 
  INTERPRETATION:
  - The first result set shows the relative prevalence of each match type:
    * If exact matches dominate, data consistency between systems is good
    * If substring matches are overly represented, it may suggest inconsistent data entry
 
  - The second result set reveals county-specific patterns:
    * Counties with unusual match type distributions may have specific data issues
    * Some counties may consistently use different record number formats
    * Historical data conversion issues may affect specific counties differently
 
  POTENTIAL ISSUES IDENTIFIED:
  - Systemic data quality problems in specific counties
  - Matching criteria that may be too aggressive or too conservative for certain counties
  - Regional variation in data entry practices */


-- Analyze match distribution by match type and county
WITH match_results AS (
    -- First condition: matching record numbers and dates
	SELECT 
		cst.recordID AS cst_recordID,
		div1.LeaseID AS div1_leaseid,
		div1.CountyName AS div1_countyName,
		'Exact Number Match' AS match_type
	FROM #LND_6732_div1_values div1
	LEFT JOIN countyScansTitle.dbo.tblexportLog tel 
		ON tel.LeaseID = div1.LeaseID
	LEFT JOIN #LND_6732_cst_values cst
		ON cst.recordNumber = div1.recordNumber 
		AND cst.fileDate = div1.RecordDate
		AND cst.countyName = div1.CountyName
	WHERE tel.leaseID IS NULL

    UNION ALL

    -- Second condition: recordNumber IS NULL, vol/page match
	SELECT 
		cst.recordID AS cst_recordID,
		div1.LeaseID AS div1_leaseid,
		div1.CountyName AS div1_countyName,
		'Vol/Page Match' AS match_type
	FROM #LND_6732_div1_values div1
	LEFT JOIN countyScansTitle.dbo.tblexportLog tel 
		ON tel.LeaseID = div1.LeaseID
	LEFT JOIN #LND_6732_cst_values cst
		 ON cst.fileDate = div1.RecordDate
		AND cst.countyName = div1.CountyName
		AND cst.volume = div1.volume 
		AND cst.page = div1.page
	WHERE tel.leaseID IS NULL

)
-- Count matches by type
SELECT 
    match_type,
    COUNT(*) AS match_count,
    CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) AS percentage
FROM match_results
GROUP BY match_type
ORDER BY match_count DESC;


-- How is it defining the categories
-- Investigate the records that are marked Low or Medium 
/* HOLISTIC QUALITY REPORT
   ---------------------------------
 
  PURPOSE:
  This comprehensive query generates a high-level quality report summarizing multiple aspects
  of the matching process. It consolidates key metrics to help:
    1. Provide an executive summary of the matching quality
    2. Track the distribution of different match types and confidence levels
    3. Identify systematic quality issues that require attention
 
  HOW IT WORKS:
  1. The query creates a common table expression (CTE) called 'match_stats' that combines
     three different analysis types:
 
     A. Match Type Distribution:
        - Groups matches by their type (exact, volume-record, substring)
        - Counts occurrences of each match type
 
     B. Multiple Match Issues:
        - Identifies the count of CST records with multiple DIV1 matches
        - This is a quality indicator that highlights potential false positives
 
     C. Match Confidence Distribution:
        - Calculates confidence scores using the same logic as match_confidence_scoring.sql
        - Groups these scores into categories: Very High, High, Medium, and Low
        - Counts matches in each confidence category
 
  2. The outer query then:
     - Displays each metric category
     - Shows the raw count for each category
     - Calculates the percentage of each category within its metric group
     - Orders results by metric and then by count (descending)
 
  INTERPRETATION:
  - Match Type Distribution: Shows the relative prevalence of each match type
    * Heavy reliance on lower-confidence match types may indicate data inconsistencies
 
  - Multiple Match Issues: Quantifies potential data quality problems
    * High counts suggest the matching may be too aggressive or data has duplicates
 
  - Match Confidence Distribution: Shows the reliability of the matches
    * Ideally, Most matches should fall in the Very High or High categories
    * Large percentages in Low category suggest potential quality issues
 
  POTENTIAL ISSUES IDENTIFIED:
  - Overall match quality based on confidence score distribution
  - Prevalence of potentially problematic match types
  - Scale of the multiple-match problem */

-- Create a comprehensive quality report
WITH match_stats AS (
    -- Count by match type
    SELECT 
        'Match Type Distribution' AS metric,
        CASE 
            WHEN cst.recordNumber = div1.recordNumber THEN 'Exact Number Match'
            WHEN cst.volume = div1.recordNumber THEN 'Volume-Page Match'
        END AS category,
        COUNT(*) AS count_value
    FROM #LND_6732_div1_values div1 
	JOIN #LND_6732_cst_values cst
        ON (cst.bigint_recordNumber = div1.bigint_recordNumber AND cst.fileDate = div1.RecordDate)
        OR (cst.volume = div1.bigint_recordNumber AND cst.fileDate = div1.RecordDate)
    LEFT JOIN countyScansTitle.dbo.tblexportLog tel 
        ON cst.recordID = tel.recordID
    WHERE tel.leaseID IS NULL
    GROUP BY 
        CASE 
            WHEN cst.bigint_recordNumber = div1.bigint_recordNumber THEN 'Exact Number Match'
            WHEN cst.volume = div1.bigint_recordNumber THEN 'Volume-RecordNo Match'
        END

    UNION ALL

    -- Count of multiple matches (potential data quality issues)
    SELECT 
        'Multiple Match Issues' AS metric,
        'CST records with multiple DIV1 matches' AS category,
        COUNT(*) AS count_value
    FROM (
        SELECT 
            cst.recordID AS cst_recordID,
            COUNT(DISTINCT div1.LeaseID) AS match_count
        FROM #LND_6732_cst_values cst
        JOIN #LND_6732_div1_values div1 
            ON (cst.bigint_recordNumber = div1.bigint_recordNumber AND cst.fileDate = div1.RecordDate)
            OR (cst.volume = div1.bigint_recordNumber AND cst.fileDate = div1.RecordDate)
            OR (LEN(cst.bigint_recordNumber) <> LEN(div1.bigint_recordNumber)
                AND LEN(cst.bigint_recordNumber) >= 5 
                AND LEN(div1.bigint_recordNumber) >= 5
                AND cst.fileDate = div1.RecordDate
                AND (CHARINDEX(cst.nvarchar_recordNumber, div1.nvarchar_recordNumber) > 0 
                    OR CHARINDEX(div1.nvarchar_recordNumber, cst.nvarchar_recordNumber) > 0))
        GROUP BY cst.recordID
        HAVING COUNT(DISTINCT div1.LeaseID) > 1
    ) multi_matches

    UNION ALL

    -- Count records by match confidence score range
    SELECT 
        'Match Confidence Distribution' AS metric,
        CASE 
            WHEN confidence_score >= 120 THEN 'Very High (120+)'
            WHEN confidence_score >= 100 THEN 'High (100-119)'
            WHEN confidence_score >= 80 THEN 'Medium (80-99)'
            ELSE 'Low (<80)'
        END AS category,
        COUNT(*) AS count_value
    FROM (
        SELECT 
            CAST(
                CASE WHEN cst.bigint_recordNumber = div1.bigint_recordNumber THEN 100
                     WHEN cst.volume = div1.bigint_recordNumber THEN 70
                     ELSE 40
                END + 
                CASE WHEN cst.volume = div1.volume AND cst.volume IS NOT NULL THEN 15 ELSE 0 END +
                CASE WHEN cst.page = div1.page AND cst.page IS NOT NULL THEN 15 ELSE 0 END +
                CASE WHEN cst.fileDate = div1.RecordDate THEN 30 ELSE 0 END
            AS INT) AS confidence_score
        FROM #LND_6732_cst_values cst
        JOIN #LND_6732_div1_values div1 
            ON (cst.bigint_recordNumber = div1.bigint_recordNumber AND cst.fileDate = div1.RecordDate)
            OR (cst.volume = div1.bigint_recordNumber AND cst.fileDate = div1.RecordDate)
            OR (LEN(cst.bigint_recordNumber) <> LEN(div1.bigint_recordNumber)
                AND LEN(cst.bigint_recordNumber) >= 5 
                AND LEN(div1.bigint_recordNumber) >= 5
                AND cst.fileDate = div1.RecordDate
                AND (CHARINDEX(cst.nvarchar_recordNumber, div1.nvarchar_recordNumber) > 0 
                    OR CHARINDEX(div1.nvarchar_recordNumber, cst.nvarchar_recordNumber) > 0))
    ) confidence_scores
    GROUP BY 
        CASE 
            WHEN confidence_score >= 120 THEN 'Very High (120+)'
            WHEN confidence_score >= 100 THEN 'High (100-119)'
            WHEN confidence_score >= 80 THEN 'Medium (80-99)'
            ELSE 'Low (<80)'
        END
)
SELECT 
    metric,
    category,
    count_value,
    CAST(100.0 * count_value / SUM(count_value) OVER(PARTITION BY metric) AS DECIMAL(5,2)) AS percentage
FROM match_stats
ORDER BY metric, count_value DESC;

-- Review this
-- GroupBy match_type
/* MATCH CRITERIA OVERLAP ANALYSIS
   ------------------------------
  
  PURPOSE:
  This query identifies records that are matched by multiple matching criteria in our join logic.
  When a record matches through multiple criteria, it could indicate either:
    1. Redundancy in the matching conditions (making some rules unnecessary)
    2. A potential data quality issue where unrelated records are being mistakenly matched
  
  HOW IT WORKS:
  1. The query creates a common table expression (CTE) called 'all_matches' containing three UNION ALL sections:
     - First section: Records matched by exact record number equality
     - Second section: Records matched by CST volume equaling DIV1 record number
     - Third section: Records matched by substring containment between record numbers
  
  2. Each record pair is tagged with a 'match_type' code (1, 2, or 3) indicating which criteria matched it
  
  3. The outer query then:
     - Groups by cst_recordID and div1_leaseid
     - Uses STRING_AGG to combine all the match_type codes that applied to each pair
     - Counts how many different match types were used
     - Filters to only show record pairs matched by more than one criterion
  
  INTERPRETATION:
  - If many records appear in the results, it suggests the matching criteria are overlapping
  - If specific match types appear together frequently, one may be redundant or could be refined
  - Records matched by all three criteria might be "obvious matches" with high data quality
  - Records matched by both criteria 1 and 3 are expected (substring match should catch exact matches too)
  
  POTENTIAL ISSUES IDENTIFIED:
  - Inefficiency in matching logic if many pairs match multiple criteria
  - Opportunity to refine or prioritize match criteria
  - Potential false positives if low-confidence criteria (like substring match) 
    pull in pairs that also match by high-confidence criteria */

-- Check if records are being matched by multiple criteria 
-- (could indicate redundant matching conditions or potential false positives)
WITH all_matches AS (
    -- First condition matches (exact record number)
    SELECT 
        cst.recordID AS cst_recordID,
        div1.LeaseID AS div1_leaseid,
        1 AS match_type
    FROM #LND_6732_modded_cst_values cst
    JOIN #LND_6732_modded_div1_values div1 
        ON cst.bigint_recordNumber = div1.bigint_recordNumber 
        AND cst.fileDate = div1.RecordDate
)
SELECT 
    cst_recordID,
    div1_leaseid,
    STRING_AGG(CAST(match_type AS VARCHAR), ', ') AS matched_by_criteria,
    COUNT(*) AS match_count
FROM all_matches
GROUP BY cst_recordID, div1_leaseid
HAVING COUNT(*) > 1
ORDER BY match_count DESC;



/* MULTIPLE MATCH DETECTION
   -----------------------
 
  PURPOSE:
  This query identifies one-to-many and many-to-one relationships between CST records and DIV1 records.
  In a perfect matching scenario, there should be a one-to-one relationship between systems. 
  Multiple matches could indicate:
    1. Duplicate records in one of the systems
    2. Over-broad matching criteria causing false positives
    3. Genuine cases where one record in system A corresponds to multiple in system B
 
  HOW IT WORKS:
  1. The query creates a common table expression (CTE) called 'all_matches' that combines all three
     match conditions (exact record number, volume as record number, and substring containment)
     to identify all possible matched pairs between CST and DIV1 systems
 
  2. The first part of the main query then:
     - Groups by cst_recordID 
     - Counts how many distinct DIV1 leaseIDs match each CST record
     - Uses STRING_AGG to list all the DIV1 leaseIDs matching each CST record
     - Filters to show only CST records that match multiple DIV1 records
 
  3. The UNION ALL combines this with the second part that:
     - Groups by div1_leaseid
     - Counts how many distinct CST recordIDs match each DIV1 record
     - Uses STRING_AGG to list all the CST recordIDs matching each DIV1 record
     - Filters to show only DIV1 records that match multiple CST records
 
  INTERPRETATION:
  - CST records matching multiple DIV1 records may indicate that the matching criteria are too loose
  - DIV1 records matching multiple CST records could suggest duplicate data in the CST system
  - High counts of duplicate matches suggest systematic issues rather than isolated data problems
  - The STRING_AGG lists help identify patterns in the multiple matches
 
  POTENTIAL ISSUES IDENTIFIED:
  - False positives in the matching logic that need refinement
  - Data quality issues where the same record is entered multiple times in one system
  - Business process issues where one record genuinely corresponds to multiple in another system
    (requiring special handling in the integration) */

-- Check if any CST record matches multiple DIV1 records
-- For recordNumber = recordNumber, countyName = countyName & fileDate = recordDate
-- There are instances where it says one match but still seems like its multiples, needs to be investigated
WITH all_matches AS (
	SELECT 
		cst.recordID AS cst_recordID,
		div1.LeaseID AS div1_leaseid,
		div1.countyName AS countyName
	FROM #LND_6732_div1_values div1
	LEFT JOIN countyScansTitle.dbo.tblexportLog tel 
		ON tel.LeaseID = div1.LeaseID
	LEFT JOIN #LND_6732_cst_values cst
		ON cst.recordNumber = div1.recordNumber 
		AND cst.fileDate = div1.RecordDate
		AND cst.countyName = div1.CountyName
	WHERE tel.leaseID IS NULL
)
-- CST records matching multiple DIV1 records
SELECT 
    CONVERT(NVARCHAR(50), cst_recordID) AS record_id, countyName,
    COUNT(DISTINCT div1_leaseid) AS match_count,
    'CST record matching multiple DIV1 records' AS match_type,
    STRING_AGG(CONVERT(NVARCHAR(MAX), div1_leaseid), ', ') AS matched_ids
FROM all_matches
GROUP BY cst_recordID, countyName
HAVING COUNT(DISTINCT div1_leaseid) > 1
UNION ALL
-- DIV1 records matching multiple CST records
SELECT 
    CONVERT(NVARCHAR(50), div1_leaseid) AS record_id,  countyName,
    COUNT(DISTINCT cst_recordID) AS match_count,
    'DIV1 record matching multiple CST records' AS match_type,
    STRING_AGG(CONVERT(NVARCHAR(MAX), cst_recordID), ', ') AS matched_ids
FROM all_matches
WHERE div1_leaseid IS NOT NULL
GROUP BY div1_leaseid, countyName
HAVING COUNT(DISTINCT cst_recordID) > 1
UNION ALL
-- CST records matching one DIV1 record
SELECT 
    CONVERT(NVARCHAR(50), cst_recordID) AS record_id,  countyName,
    COUNT(DISTINCT div1_leaseid) AS match_count,
    'CST record matching one DIV1 record' AS match_type,
    STRING_AGG(CONVERT(NVARCHAR(MAX), div1_leaseid), ', ') AS matched_ids
FROM all_matches
WHERE cst_recordID IS NOT NULL AND div1_leaseid IS NOT NULL
GROUP BY cst_recordID, countyName
HAVING COUNT(DISTINCT div1_leaseid) = 1
UNION ALL
-- DIV1 records matching one CST record
SELECT 
    CONVERT(NVARCHAR(50), div1_leaseid) AS record_id, countyName,
    COUNT(DISTINCT cst_recordID) AS match_count,
    'DIV1 record matching one CST record' AS match_type,
    STRING_AGG(CONVERT(NVARCHAR(MAX), cst_recordID), ', ') AS matched_ids
FROM all_matches
WHERE div1_leaseid IS NOT NULL AND cst_recordID IS NOT NULL
GROUP BY div1_leaseid, countyName
HAVING COUNT(DISTINCT cst_recordID) = 1;



-- For volume = volume & page = page, countyName = countyName & fileDate = recordDate
WITH all_matches AS (
	SELECT 
		cst.recordID AS cst_recordID,
		div1.LeaseID AS div1_leaseid,
		div1.countyName AS countyName
	FROM #LND_6732_div1_values div1
	LEFT JOIN countyScansTitle.dbo.tblexportLog tel 
		ON tel.LeaseID = div1.LeaseID
	LEFT JOIN #LND_6732_cst_values cst
		 ON cst.fileDate = div1.RecordDate
		AND cst.countyName = div1.CountyName
		AND cst.volume = div1.volume 
		AND cst.page = div1.page
	WHERE tel.leaseID IS NULL
)
-- CST records matching multiple DIV1 records
SELECT 
    CONVERT(NVARCHAR(50), cst_recordID) AS record_id, countyName,
    COUNT(DISTINCT div1_leaseid) AS match_count,
    'CST record matching multiple DIV1 records' AS match_type,
    STRING_AGG(CONVERT(NVARCHAR(MAX), div1_leaseid), ', ') AS matched_ids
FROM all_matches
GROUP BY cst_recordID, countyName
HAVING COUNT(DISTINCT div1_leaseid) > 1
UNION ALL
-- DIV1 records matching multiple CST records
SELECT 
    CONVERT(NVARCHAR(50), div1_leaseid) AS record_id, countyName,
    COUNT(DISTINCT cst_recordID) AS match_count,
    'DIV1 record matching multiple CST records' AS match_type,
    STRING_AGG(CONVERT(NVARCHAR(MAX), cst_recordID), ', ') AS matched_ids
FROM all_matches
WHERE div1_leaseid IS NOT NULL
GROUP BY div1_leaseid, countyName
HAVING COUNT(DISTINCT cst_recordID) > 1
UNION ALL
-- CST records matching one DIV1 record
SELECT 
    CONVERT(NVARCHAR(50), cst_recordID) AS record_id, countyName,
    COUNT(DISTINCT div1_leaseid) AS match_count,
    'CST record matching one DIV1 record' AS match_type,
    STRING_AGG(CONVERT(NVARCHAR(MAX), div1_leaseid), ', ') AS matched_ids
FROM all_matches
WHERE cst_recordID IS NOT NULL AND div1_leaseid IS NOT NULL
GROUP BY cst_recordID, countyName
HAVING COUNT(DISTINCT div1_leaseid) = 1
UNION ALL
-- DIV1 records matching one CST record
SELECT 
    CONVERT(NVARCHAR(50), div1_leaseid) AS record_id, countyName,
    COUNT(DISTINCT cst_recordID) AS match_count,
    'DIV1 record matching one CST record' AS match_type,
    STRING_AGG(CONVERT(NVARCHAR(MAX), cst_recordID), ', ') AS matched_ids
FROM all_matches
WHERE div1_leaseid IS NOT NULL AND cst_recordID IS NOT NULL
GROUP BY div1_leaseid, countyName
HAVING COUNT(DISTINCT cst_recordID) = 1;


SELECT tc.CountyName, *
FROM [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] tll
LEFT JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblCounty] tc ON tc.CountyID = tll.CountyID
WHERE leaseID IN (3255742)

SELECT tel.*, tr.*
FROM countyScansTitle.dbo.tblrecord tr
LEFT JOIN countyScansTitle.dbo.tblexportLog tel ON tr.recordID = tel.recordID
WHERE tr.recordID = 'f91efc4c-18b0-425c-b7a6-f46068b74521'

SELECT *
FROM [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblLegalLease] tll
LEFT JOIN [AUS2-DIV1-DDB01].[div1_daily].[dbo].[tblCounty] tc ON tc.CountyID = tll.CountyID
WHERE tll.leaseID IN (3255742, 53124, 270757, 88021, 3338670, 3383938, 3575412, 3641198, 809245, 809259, 3501948, 4627400, 1113262, 1139364, 1770697, 1648105, 1648107, 1648108, 1648109, 1648110, 1648111, 1648113, 1648114, 1648285, 1648286, 1648287, 1648288, 1669585, 1669586, 1669587, 1669588, 1669589, 1669590, 1669591, 1669592, 1669593, 1669594, 1669595, 1669596, 1669597, 1669598, 1669599, 1669600, 1669601, 1669602, 1669603, 1669604, 1669605, 1669606, 1669607, 1669608, 1669609, 1669610, 1432276, 1808968, 1830774, 3891542, 3891627, 1850664, 1948092, 2147166, 3938896, 2355858, 2562600, 5121106, 5121112, 5121121, 5121123, 5121134, 5121132, 5121147, 5121148, 5121149, 5121150, 5121151, 5123217, 5123219, 5123223, 5123231, 5123232, 5123233, 5123235, 5123237, 5123238, 5123239, 5123227, 5123241, 5123242)


/*******************************************************************************
* QUERY NAME: Many-to-One Relationship Analysis
* AUTHOR: Donald Massey
* DATE: 2025-03-17
*
* DESCRIPTION:
* This query analyzes and quantifies the many-to-one relationships between 
* CST and DIV1 tables. It identifies which records have multiple matches and,
* most importantly, explains WHY these multiple matches are occurring by 
* categorizing the specific match conditions responsible.
*
* INPUT TABLES:
* - #LND_6732_modded_cst_values: Contains CST records with various identifiers
* - #LND_6732_modded_div1_values: Contains DIV1 records with various identifiers
*
* OUTPUT:
* The query returns three result sets:
* 1. Match condition summary: Shows frequency of each match condition
* 2. CST records with multiple DIV1 matches: Details CST records matching multiple DIV1s
* 3. DIV1 records with multiple CST matches: Details DIV1 records matching multiple CSTs
*
* MATCH CONDITIONS EXPLAINED:
* - Condition 1: Direct match on bigint_recordNumber, fileDate, and countyName
* - Condition 2: Match between CST volume and DIV1 bigint_recordNumber
* - Condition 3A: CST record number is a substring of DIV1 record number
* - Condition 3B: DIV1 record number is a substring of CST record number
*******************************************************************************/

-- Step 1: Create detailed matching information with condition classification
WITH match_details AS (
    SELECT 
        cst.recordID AS cst_recordID,
        div1.LeaseID AS div1_leaseid,
        cst.bigint_recordNumber AS cst_bigint_recordNumber,
        div1.bigint_recordNumber AS div1_bigint_recordNumber,
        cst.nvarchar_recordNumber AS cst_nvarchar_recordNumber,
        div1.nvarchar_recordNumber AS div1_nvarchar_recordNumber,
        cst.volume AS cst_volume,
        cst.fileDate AS cst_fileDate,
        div1.RecordDate AS div1_RecordDate,
        cst.countyName AS cst_countyName,
        -- Classify each match by the specific condition that caused it
        CASE
            -- Condition 1: Direct match on bigint_recordNumber
            WHEN cst.bigint_recordNumber = div1.bigint_recordNumber AND cst.fileDate = div1.RecordDate AND cst.countyName = div1.countyName
                THEN 'Condition 1: bigint_recordNumber match'
            -- Condition 2: Match between CST volume and DIV1 bigint_recordNumber
            WHEN cst.volume = div1.bigint_recordNumber AND cst.fileDate = div1.RecordDate AND cst.countyName = div1.countyName
                THEN 'Condition 2: volume to bigint_recordNumber match'
            -- Condition 3A: CST record number is contained within DIV1 record number
            WHEN LEN(cst.bigint_recordNumber) <> LEN(div1.bigint_recordNumber)
                AND LEN(cst.bigint_recordNumber) >= 5 
                AND LEN(div1.bigint_recordNumber) >= 5
                AND cst.fileDate = div1.RecordDate
                AND cst.countyName = div1.countyName
                AND CHARINDEX(cst.nvarchar_recordNumber, div1.nvarchar_recordNumber) > 0
                THEN 'Condition 3A: cst record number found within div1 record number'
            -- Condition 3B: DIV1 record number is contained within CST record number
            WHEN LEN(cst.bigint_recordNumber) <> LEN(div1.bigint_recordNumber)
                AND LEN(cst.bigint_recordNumber) >= 5 
                AND LEN(div1.bigint_recordNumber) >= 5
                AND cst.fileDate = div1.RecordDate
                AND cst.countyName = div1.countyName
                AND CHARINDEX(div1.nvarchar_recordNumber, cst.nvarchar_recordNumber) > 0
                THEN 'Condition 3B: div1 record number found within cst record number'
            ELSE 'Unknown match condition'
        END AS match_condition
    FROM #LND_6732_modded_cst_values cst
    -- Join using the same complex criteria that led to the many-to-one relationships
    JOIN #LND_6732_modded_div1_values div1 
        ON ((cst.bigint_recordNumber = div1.bigint_recordNumber AND cst.fileDate = div1.RecordDate AND cst.countyName = div1.countyName)
        OR (cst.volume = div1.bigint_recordNumber AND cst.fileDate = div1.RecordDate AND cst.countyName = div1.countyName)
        OR (LEN(cst.bigint_recordNumber) <> LEN(div1.bigint_recordNumber)
            AND LEN(cst.bigint_recordNumber) >= 5 
            AND LEN(div1.bigint_recordNumber) >= 5
            AND cst.fileDate = div1.RecordDate
            AND cst.countyName = div1.countyName
            AND (CHARINDEX(cst.nvarchar_recordNumber, div1.nvarchar_recordNumber) > 0 
                OR CHARINDEX(div1.nvarchar_recordNumber, cst.nvarchar_recordNumber) > 0)))
),

-- Step 2: Identify CST records that match multiple DIV1 records with detailed match reasons
cst_multiple_matches AS (
    SELECT 
        cst_recordID,
        COUNT(DISTINCT div1_leaseid) AS match_count, -- Count of DIV1 matches
        'CST record matching multiple DIV1 records' AS match_type,
        -- Comma-separated list of matched DIV1 IDs
        STRING_AGG(CONVERT(NVARCHAR(MAX), div1_leaseid), ', ') AS matched_ids,
        -- Comma-separated list of match conditions that caused the matches
        STRING_AGG(CONVERT(NVARCHAR(MAX), match_condition), ', ') WITHIN GROUP (ORDER BY match_condition) AS match_conditions,
        -- Detailed breakdown of each match, formatted for readability
        STRING_AGG(CONVERT(NVARCHAR(MAX), 
            'DIV1 ID: ' + CONVERT(NVARCHAR(50), div1_leaseid) + 
            ', Match: ' + match_condition + 
            ', Record#: ' + ISNULL(div1_nvarchar_recordNumber, 'NULL')
        ), CHAR(13) + CHAR(10)) AS match_details
    FROM match_details
    GROUP BY cst_recordID
    -- Only include records that match multiple DIV1 records
    HAVING COUNT(DISTINCT div1_leaseid) > 1
),

-- Step 3: Identify DIV1 records that match multiple CST records with detailed match reasons
div1_multiple_matches AS (
    SELECT 
        div1_leaseid,
        COUNT(DISTINCT cst_recordID) AS match_count, -- Count of CST matches
        'DIV1 record matching multiple CST records' AS match_type,
        -- Comma-separated list of matched CST IDs
        STRING_AGG(CONVERT(NVARCHAR(MAX), cst_recordID), ', ') AS matched_ids,
        -- Comma-separated list of match conditions that caused the matches
        STRING_AGG(CONVERT(NVARCHAR(MAX), match_condition), ', ') WITHIN GROUP (ORDER BY match_condition) AS match_conditions,
        -- Detailed breakdown of each match, formatted for readability
        STRING_AGG(CONVERT(NVARCHAR(MAX), 
            'CST ID: ' + CONVERT(NVARCHAR(50), cst_recordID) + 
            ', Match: ' + match_condition + 
            ', Record#: ' + ISNULL(cst_nvarchar_recordNumber, 'NULL')
        ), CHAR(13) + CHAR(10)) AS match_details
    FROM match_details
    GROUP BY div1_leaseid
    -- Only include records that match multiple CST records
    HAVING COUNT(DISTINCT cst_recordID) > 1
),

-- Add a new CTE to calculate the total for percentages
match_condition_totals AS (
    SELECT 
        match_condition,
        COUNT(*) AS condition_count,
        SUM(COUNT(*)) OVER() AS total_count
    FROM match_details
    GROUP BY match_condition
)

-- RESULT SET 1: Summary of match conditions causing many-to-one relationships
SELECT 
    'Match condition summary' AS analysis_type,
    match_condition AS record_id,
    condition_count AS match_count,
    -- Pre-calculated percentage to avoid window function in UNION
    CAST(condition_count * 100.0 / total_count AS NVARCHAR(100)) AS match_details
FROM match_condition_totals

UNION ALL

-- RESULT SET 2: Detailed analysis of CST records with multiple DIV1 matches
SELECT
    'CST records with multiple DIV1 matches' AS analysis_type,
    CONVERT(NVARCHAR(255), cst_recordID) AS record_id,
    match_count,
    match_details
FROM cst_multiple_matches

UNION ALL

-- RESULT SET 3: Detailed analysis of DIV1 records with multiple CST matches
SELECT
    'DIV1 records with multiple CST matches' AS analysis_type,
    CONVERT(NVARCHAR(255), div1_leaseid) AS record_id,
    match_count,
    match_details
FROM div1_multiple_matches
ORDER BY match_count DESC;

/*******************************************************************************
* HOW TO USE THE RESULTS:
*
* 1. First examine the "Match condition summary" to understand which conditions
*    are causing the most many-to-one relationships.
*
* 2. Then look at the "CST records with multiple DIV1 matches" or 
*    "DIV1 records with multiple CST matches" sections to investigate specific
*    problem records in detail.
*
* 3. Pay special attention to:
*    - Records with very high match counts
*    - Patterns in the match conditions
*    - Counties where this happens more frequently
*
* 4. Use this information to refine the join criteria for a more precise 
*    one-to-one mapping between CST and DIV1 records.
*
* POTENTIAL SOLUTIONS BASED ON RESULTS:
* - If Condition 3A/3B (substring matches) cause many issues, consider making
*   the substring matching more restrictive
* - If Condition 2 (volume matching) is problematic, validate if this is actually
*   a valid match criterion
* - Consider adding additional match criteria like document type
* - For counties with high duplicate rates, potentially develop county-specific
*   matching rules
*******************************************************************************/

-- Analysis by county
SELECT 
    cst_countyName AS county_name,
    match_condition,
    COUNT(*) AS match_count,
    COUNT(DISTINCT cst_recordID) AS unique_cst_records,
    COUNT(DISTINCT div1_leaseid) AS unique_div1_records,
    COUNT(*) - COUNT(DISTINCT cst_recordID) AS cst_duplicate_matches,
    COUNT(*) - COUNT(DISTINCT div1_leaseid) AS div1_duplicate_matches
FROM match_details
GROUP BY cst_countyName, match_condition
ORDER BY cst_countyName, COUNT(*) DESC;


/* MATCH CONFIDENCE SCORING
   -----------------------
 
  PURPOSE:
  This query implements a scoring system to evaluate the confidence level of each match between
  CST and DIV1 records. Not all matches are created equal - some have stronger evidence than others.
  By calculating a confidence score, we can:
    1. Prioritize high-confidence matches for processing
    2. Apply additional verification to low-confidence matches
    3. Establish thresholds for automatic vs. manual review
 
  HOW IT WORKS:
  1. The query joins CST and DIV1 records using all three match conditions combined
     (exact record number, volume as record number, and substring containment)
 
  2. For each matched pair, it calculates a confidence score based on:
     - Match type base score:
       * 100 points: Exact record number match (highest confidence)
       * 70 points:  Volume matches record number (medium confidence)
       * 40 points:  Substring match only (lowest confidence)
     
     - Additional points for matching attributes:
       * +15 points: Volume numbers match
       * +15 points: Page numbers match
       * +30 points: Dates match (this should always be true given the join conditions)
 
  3. The maximum possible score is 160 (exact record number + all attributes match)
     and the minimum is 40 (substring match only, with matching date which is required)
 
  4. The query also captures which specific match type was used and includes details of all
     relevant fields for manual review
 
  INTERPRETATION:
  - Higher scores (130-160) indicate very reliable matches with multiple confirming attributes
  - Medium scores (100-129) are still good matches but with fewer confirming details
  - Lower scores (40-99) should be treated with caution and may need manual verification
  - The distribution of scores provides insight into the overall quality of the matching
  - Records with the same text fields but different scores can help identify missing data
 
  POTENTIAL ISSUES IDENTIFIED:
  - Low-confidence matches that might be false positives
  - Inconsistent data entry patterns between systems
  - Opportunities to refine matching logic based on scoring patterns */

-- Create a confidence score for each match based on how many attributes match
SELECT 
    cst.recordID AS cst_recordID,
    div1.LeaseID AS div1_leaseid,
    -- Calculate match score (higher = better match)
    CAST(
        CASE WHEN cst.bigint_recordNumber = div1.bigint_recordNumber THEN 100 -- Exact record number match
             WHEN cst.volume = div1.bigint_recordNumber THEN 70 -- Volume matches record number
             ELSE 40 -- Substring match only
        END + 
        CASE WHEN cst.volume = div1.volume AND cst.volume IS NOT NULL THEN 15 ELSE 0 END +
        CASE WHEN cst.page = div1.page AND cst.page IS NOT NULL THEN 15 ELSE 0 END +
        CASE WHEN cst.fileDate = div1.RecordDate THEN 30 ELSE 0 END
    AS INT) AS match_confidence_score,
    -- Match reason
    CASE 
        WHEN cst.bigint_recordNumber = div1.bigint_recordNumber THEN 'Exact record number match'
        WHEN cst.volume = div1.bigint_recordNumber THEN 'Volume matches record number'
        ELSE 'Substring match'
    END AS match_type,
    cst.bigint_recordNumber AS cst_recordno,
    div1.bigint_recordNumber AS div1_recordno,
    cst.volume AS cst_volume,
    div1.volume AS div1_volume,
    cst.page AS cst_page,
    div1.page AS div1_page,
    cst.fileDate, 
    div1.RecordDate
FROM #LND_6732_modded_cst_values cst
JOIN #LND_6732_modded_div1_values div1 
    ON (cst.bigint_recordNumber = div1.bigint_recordNumber AND cst.fileDate = div1.RecordDate)
    OR (cst.volume = div1.bigint_recordNumber AND cst.fileDate = div1.RecordDate)
    OR (LEN(cst.bigint_recordNumber) <> LEN(div1.bigint_recordNumber)
        AND LEN(cst.bigint_recordNumber) >= 5 
        AND LEN(div1.bigint_recordNumber) >= 5
        AND cst.fileDate = div1.RecordDate
        AND (CHARINDEX(cst.nvarchar_recordNumber, div1.nvarchar_recordNumber) > 0 
            OR CHARINDEX(div1.nvarchar_recordNumber, cst.nvarchar_recordNumber) > 0))
ORDER BY match_confidence_score DESC;


-- Ask claude how you could best utilize this query, something like getting the counts of the potential_issue for investigation, etc.
/* POTENTIAL MISSED MATCHES ANALYSIS
   --------------------------------
 
  PURPOSE:
  This query searches for records that could potentially be matches but were missed by the
  current matching criteria. It helps:
    1. Identify gaps in the existing matching logic
    2. Discover edge cases that current rules don't handle
    3. Evaluate whether matching criteria should be expanded or refined
 
  HOW IT WORKS:
  1. The query joins CST and DIV1 records where:
     - The dates match (essential for any valid match)
     - Record numbers exist in both systems (not NULL)
     - Record numbers are NOT exact matches (these are already caught)
     - Volume doesn't match record number (these are already caught)
 
  2. It then looks for pairs with these characteristics:
     - First and last 3 digits of the record number match (suggesting typos in the middle)
       OR
     - Record numbers differ by less than 100 and have same length (suggesting minor transcription errors)
 
  3. It explicitly excludes records already caught by the substring matching condition
 
  4. For each potential missed match, it adds a 'potential_issue' field explaining why the
     current matching criteria might have missed it:
     - "Length difference too large" (for records with very different lengths)
     - "Record numbers too short" (for records with fewer than 5 digits)
     - "Other reason - manual review needed" (for other cases)
 
  INTERPRETATION:
  - If many records appear with similar patterns, consider adding new matching rules
  - Pay attention to the "potential_issue" field to understand why matches are being missed
  - Clusters of missed matches from specific counties may indicate localized data issues
  - Date-specific patterns may indicate changes in data entry practices over time
 
  POTENTIAL ISSUES IDENTIFIED:
  - Gaps in matching logic that could be addressed with additional rules
  - Data entry patterns not accommodated by current matching criteria
  - Opportunities to improve match coverage without sacrificing precision */

-- Find records that could potentially match but aren't caught by current criteria
-- Look for records with same date and similar record numbers that aren't matched
SELECT 
    cst.recordID AS cst_recordID,
    cst.recordNumber AS cst_recordNumber,
    cst.bigint_recordNumber AS cst_clean_recordno,
    div1.LeaseID AS div1_leaseid,
    div1.recordNumber AS div1_recordNumber,  
    div1.bigint_recordNumber AS div1_clean_recordno,
    cst.volume AS cst_volume,
    cst.page AS cst_page,
    div1.volume AS div1_volume,
    div1.page AS div1_page,
    cst.fileDate,
    div1.RecordDate,
    -- Potential reasons this match might be missed
    CASE
        WHEN ABS(LEN(cst.bigint_recordNumber) - LEN(div1.bigint_recordNumber)) > 3 THEN 'Length difference too large'
        WHEN LEN(cst.bigint_recordNumber) < 5 OR LEN(div1.bigint_recordNumber) < 5 THEN 'Record numbers too short'
        ELSE 'Other reason - manual review needed'
    END AS potential_issue
FROM #LND_6732_modded_cst_values cst
JOIN #LND_6732_modded_div1_values div1 
    ON cst.fileDate = div1.RecordDate
    AND cst.bigint_recordNumber IS NOT NULL
    AND div1.bigint_recordNumber IS NOT NULL
    -- Check for similarity but not exact match
    AND cst.bigint_recordNumber <> div1.bigint_recordNumber
    -- Exclude records already matched by volume
    AND (cst.volume <> div1.bigint_recordNumber OR cst.volume IS NULL)
    -- Similar numbers but not caught by substring matching
    AND (
        -- First and last 3 digits match
        (RIGHT(cst.bigint_recordNumber, 3) = RIGHT(div1.bigint_recordNumber, 3) AND
         LEFT(cst.bigint_recordNumber, 3) = LEFT(div1.bigint_recordNumber, 3))
        OR
        -- Record numbers differ by just 1-2 digits
        (ABS(cst.bigint_recordNumber - div1.bigint_recordNumber) < 100 AND 
         LEN(cst.bigint_recordNumber) = LEN(div1.bigint_recordNumber))
    )
    -- Not already matched by the substring condition
    AND NOT (
        LEN(cst.bigint_recordNumber) <> LEN(div1.bigint_recordNumber)
        AND LEN(cst.bigint_recordNumber) >= 5 
        AND LEN(div1.bigint_recordNumber) >= 5
        AND (CHARINDEX(cst.nvarchar_recordNumber, div1.nvarchar_recordNumber) > 0 
             OR CHARINDEX(div1.nvarchar_recordNumber, cst.nvarchar_recordNumber) > 0)
    )
LEFT JOIN countyScansTitle.dbo.tblexportLog tel 
    ON cst.recordID = tel.recordID
WHERE tel.leaseID IS NULL
ORDER BY cst.fileDate, cst.bigint_recordNumber;



/* EDGE CASE DETECTION AND FALSE POSITIVE ANALYSIS
   ---------------------------------------------
 
  PURPOSE:
  This query focuses on identifying potential false positives in the substring matching condition,
  which is the most likely source of incorrect matches. It helps:
    1. Find matches with weak substring relationships that might be coincidental
    2. Quantify the strength of substring matches using a percentage measure
    3. Identify high-risk matches that may need additional verification
 
  HOW IT WORKS:
  1. The query selects only records matched by the substring condition:
     - Different length record numbers
     - At least 5 digits in each
     - Matching dates
     - One record number contains the other as a substring
 
  2. For each match, it calculates a "match_percentage":
     - Takes the length of the shorter record number
     - Divides by the length of the longer record number
     - Multiplies by 100 to get a percentage
     - Example: "12345" matching "123456789" would be (5/9)*100 = 55.56%
 
  3. The results are filtered to show only matches with less than 70% match percentage,
     as these represent the highest risk of being false positives
 
  4. Results are ordered by match percentage (lowest first)
 
  INTERPRETATION:
  - Very low percentages (under 50%) suggest highly questionable matches
  - Patterns in the record numbers can reveal if certain digit sequences are causing false matches
  - If many low-percentage matches appear, consider increasing the minimum substring length
    requirement or adding other constraints to the substring matching condition
 
  POTENTIAL ISSUES IDENTIFIED:
  - Substring matches that are coincidental rather than meaningful
  - Common numeric sequences causing false associations (e.g., year portions in dates)
  - Need for additional filtering to eliminate weak substring matches */

-- Identify potential false positives in substring matching
-- Short substrings can lead to incorrect matches
SELECT 
    cst.recordID AS cst_recordID,
    div1.LeaseID AS div1_leaseid,
    cst.recordNumber AS cst_recordNumber,
    div1.recordNumber AS div1_recordNumber,
    cst.nvarchar_recordNumber AS cst_clean_recordno,
    div1.nvarchar_recordNumber AS div1_clean_recordno,
    cst.fileDate, 
    div1.RecordDate,
    -- Calculate what percentage of the longer string matches
    CAST(
        CASE 
            WHEN LEN(cst.nvarchar_recordNumber) > LEN(div1.nvarchar_recordNumber) 
            THEN 100.0 * LEN(div1.nvarchar_recordNumber) / LEN(cst.nvarchar_recordNumber)
            ELSE 100.0 * LEN(cst.nvarchar_recordNumber) / LEN(div1.nvarchar_recordNumber)
        END AS DECIMAL(5,2)
    ) AS match_percentage
FROM #LND_6732_modded_cst_values cst
JOIN #LND_6732_modded_div1_values div1 
    ON LEN(cst.bigint_recordNumber) <> LEN(div1.bigint_recordNumber)
    AND LEN(cst.bigint_recordNumber) >= 5 
    AND LEN(div1.bigint_recordNumber) >= 5
    AND cst.fileDate = div1.RecordDate
    AND (
        CHARINDEX(cst.nvarchar_recordNumber, div1.nvarchar_recordNumber) > 0 
        OR CHARINDEX(div1.nvarchar_recordNumber, cst.nvarchar_recordNumber) > 0
    )
-- Focus on potential false positives where match percentage is low
WHERE (
    CASE 
        WHEN LEN(cst.nvarchar_recordNumber) > LEN(div1.nvarchar_recordNumber) 
        THEN 100.0 * LEN(div1.nvarchar_recordNumber) / LEN(cst.nvarchar_recordNumber)
        ELSE 100.0 * LEN(cst.nvarchar_recordNumber) / LEN(div1.nvarchar_recordNumber)
    END
) < 70 -- Less than 70% match
ORDER BY match_percentage;


/* SAMPLE GENERATION FOR MANUAL REVIEW
   ----------------------------------
 
  PURPOSE:
  This query creates stratified random samples from each match type for manual verification.
  Manual review is a critical quality control measure because it:
    1. Validates whether the algorithmic matches correspond to human judgment
    2. Reveals patterns of mismatches that may not be evident in aggregate statistics
    3. Provides concrete examples for discussion with stakeholders
 
  HOW IT WORKS:
  1. The query is divided into three separate SELECT statements, one for each match type:
     - First query: Samples records matched by exact record number equality
     - Second query: Samples records matched by CST volume equaling DIV1 record number
     - Third query: Samples records matched by substring containment between record numbers
 
  2. Each query:
     - Limits results to 20 random records using TOP 20 and ORDER BY NEWID()
     - NEWID() generates a random UUID for each row, effectively randomizing the order
     - Includes all relevant fields needed for manual comparison
     - Tags each result set with a 'match_type' label
 
  3. The third query includes additional fields specific to substring matching:
     - cst_clean_recordno and div1_clean_recordno show the cleaned numeric portions
       to make it easier to see the substring relationship
 
  INTERPRETATION:
  - These sample sets should be exported (e.g., to Excel) for manual review by domain experts
  - Reviewers should determine if each proposed match is valid based on their knowledge
  - Calculate false positive rates for each match type: (incorrect matches / total reviewed)
  - High false positive rates for specific match types indicate those criteria need refinement
 
  POTENTIAL ISSUES IDENTIFIED:
  - Systematic matching errors in specific match types
  - Edge cases not handled by the matching algorithm
  - Variations in data quality across different match types */

-- Generate random samples from each match type for manual verification
-- Condition 1: Exact record number matches
/*
SELECT TOP 20 
    'Exact Number Match' AS match_type,
    cst.recordID AS cst_recordID,
    cst.recordNumber AS cst_recordNumber,
    div1.LeaseID AS div1_leaseid,
    div1.recordNumber AS div1_recordNumber,
    cst.volume AS cst_volume,
    cst.page AS cst_page,
    div1.volume AS div1_volume,
    div1.page AS div1_page,
    cst.fileDate, 
    div1.RecordDate
FROM #LND_6732_modded_cst_values cst
JOIN #LND_6732_modded_div1_values div1 
    ON cst.bigint_recordNumber = div1.bigint_recordNumber 
    AND cst.fileDate = div1.RecordDate
ORDER BY NEWID(); -- Random sorting

-- Condition 2: Volume matching record number
SELECT TOP 20 
    'Volume-RecordNo Match' AS match_type,
    cst.recordID AS cst_recordID,
    cst.recordNumber AS cst_recordNumber,
    div1.LeaseID AS div1_leaseid,
    div1.recordNumber AS div1_recordNumber,
    cst.volume AS cst_volume,
    cst.page AS cst_page,
    div1.volume AS div1_volume,
    div1.page AS div1_page,
    cst.fileDate, 
    div1.RecordDate
FROM #LND_6732_modded_cst_values cst
JOIN #LND_6732_modded_div1_values div1 
    ON cst.volume = div1.bigint_recordNumber 
    AND cst.fileDate = div1.RecordDate
ORDER BY NEWID(); -- Random sorting

-- Condition 3: Substring matching
SELECT TOP 20 
    'Substring Match' AS match_type,
    cst.recordID AS cst_recordID,
    cst.recordNumber AS cst_recordNumber,
    div1.LeaseID AS div1_leaseid,
    div1.recordNumber AS div1_recordNumber,
    cst.nvarchar_recordNumber AS cst_clean_recordno,
    div1.nvarchar_recordNumber AS div1_clean_recordno,
    cst.volume AS cst_volume,
    cst.page AS cst_page,
    div1.volume AS div1_volume,
    div1.page AS div1_page,
    cst.fileDate, 
    div1.RecordDate
FROM #LND_6732_modded_cst_values cst
JOIN #LND_6732_modded_div1_values div1 
    ON LEN(cst.bigint_recordNumber) <> LEN(div1.bigint_recordNumber)
    AND LEN(cst.bigint_recordNumber) >= 5 
    AND LEN(div1.bigint_recordNumber) >= 5
    AND cst.fileDate = div1.RecordDate
    AND (
        CHARINDEX(cst.nvarchar_recordNumber, div1.nvarchar_recordNumber) > 0 
        OR CHARINDEX(div1.nvarchar_recordNumber, cst.nvarchar_recordNumber) > 0
    )
ORDER BY NEWID(); -- Random sorting
*/
-- End QC Queries