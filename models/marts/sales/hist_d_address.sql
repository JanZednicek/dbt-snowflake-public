/* 
-----------------------------------------------------------------------------
  About me: 
    - Powered by Jan Zednicek - https://janzednicek.cz
    - If you find any inconsistencies, please let me know - jsem@janzednicek.cz
    - Feel free to use this code without any limitations for learning (non-commercial use)
      If you find it helpful, consider giving credit by referencing my website
  
  About this table:
    - Table:   hist_d_address.sql
    - Source:  snap_aw_address.sql
    - Description: dimension table with surrogate keys, hist table contains complete history of 
				   records from primary source (all changes) based on SCD2. Curr table contains 
				   just current version (last known version of record)
				   We assign dummy -1 surrogate key (replacing NULL references)
				   and create date validity intervals from/to for each record
    - Log
        2024-02-11 - created
-----------------------------------------------------------------------------
*/

    WITH SNAPSHOT_TABLE AS (
	-- some standard stuff like casting, renaming etc.
	SELECT 
		CAST(ROW_NUMBER() OVER (ORDER BY ADDRESSID,_FIVETRAN_SYNCED) AS INT)					AS SK_ADDRESS
		,CAST(NULL AS INT) 																		AS SK_ADDRESS_MASTER
		,CAST(ADDRESSID AS NVARCHAR(255)) 														AS ID_BUSINESS_KEY
		,CAST(NULL AS NCHAR(1)) 																AS CODE_STATUS
		,CAST(MODIFIEDDATE AS DATE)  															AS SK_DATE_MODIFIED
		,CAST(DBT_VALID_FROM AS DATE) 															AS SK_DATE_VALID_FROM
		,CAST(COALESCE(DBT_VALID_TO,'3000-12-31') AS DATE) 				   					 	AS SK_DATE_VALID_TO
		,CAST(CITY AS NVARCHAR(255))															AS TEXT_CITY
		,CAST(ADDRESSLINE1 AS NVARCHAR(255)) 													AS TEXT_ADDRESS1
		,CAST(ADDRESSLINE2  AS NVARCHAR(255))													AS TEXT_ADDRESS2
		,CAST(STATEPROVINCE AS NVARCHAR(255)) 													AS TEXT_STATEPROVINCE
		,CAST(POSTALCODE AS NVARCHAR(255)) 														AS TEXT_POSTALCODE
		,CAST(COUNTRYREGION AS NVARCHAR(255))													AS TEXT_COUNTRYREGION
	FROM {{ ref('snap_aw_address') }}
    -- STAGING.SNAP_AW_ADDRESS
	
		UNION ALL
	
	-- creating row as a dimmension for null values in referrencing table
	SELECT 
		-1 																						AS SK_ADDRESS
		,-1 																					AS SK_ADDRESS_MASTER
		,'N/A' 																					AS ID_BUSINESS_KEY
		,'C'																					AS CODE_STATUS
		,'1900-01-01'				  															AS SK_DATE_MODIFIED
		,'1900-01-01'				 															AS SK_DATE_VALID_FROM
		,'3000-12-31'										 									AS SK_DATE_VALID_TO
		,'N/A'																					AS TEXT_CITY
		,'N/A'																					AS TEXT_ADDRESS1
		,'N/A'																					AS TEXT_ADDRESS2
		,'N/A'																					AS TEXT_STATEPROVINCE
		,'N/A' 																					AS TEXT_POSTALCODE
		,'N/A'																					AS TEXT_COUNTRYREGION
	)
    SELECT 
        SK_ADDRESS
        ,MIN(SK_ADDRESS) OVER (PARTITION BY ID_BUSINESS_KEY 
							   ORDER BY SK_ADDRESS) 											AS SK_ADDRESS_MASTER
        ,ID_BUSINESS_KEY
		,CASE
			WHEN SK_DATE_VALID_TO = '3000-12-31' THEN 'C'
			ELSE 'E'
		 END																					AS CODE_STATUS 
        ,SK_DATE_MODIFIED
        ,CASE
            WHEN LAG(SK_DATE_VALID_FROM) OVER (PARTITION BY ID_BUSINESS_KEY ORDER BY SK_DATE_VALID_FROM) IS NULL 
            THEN '1900-01-01'
            ELSE SK_DATE_VALID_FROM
        END 																					AS SK_DATE_VALID_FROM_TYPE2
        ,SK_DATE_VALID_FROM
        ,SK_DATE_VALID_TO
        ,TEXT_CITY
        ,TEXT_ADDRESS1
        ,TEXT_ADDRESS2
        ,TEXT_STATEPROVINCE
        ,TEXT_POSTALCODE
        ,TEXT_COUNTRYREGION
    FROM SNAPSHOT_TABLE