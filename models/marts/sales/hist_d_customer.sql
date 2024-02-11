/* 
-----------------------------------------------------------------------------
  About me: 
    - Powered by Jan Zednicek - https://janzednicek.cz
    - If you find any inconsistencies, please let me know - jsem@janzednicek.cz
    - Feel free to use this code without any limitations for learning (non-commercial use)
      If you find it helpful, consider giving credit by referencing my website
  
  About this table:
    - Table:   hist_d_customer.sql
    - Source:  snap_aw_customer.sql
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
		CAST(ROW_NUMBER() OVER (ORDER BY CUSTOMERID,_FIVETRAN_SYNCED) AS INT)					AS SK_CUSTOMER
		,CAST(NULL AS INT) 																		AS SK_CUSTOMER_MASTER
		,CAST(CUSTOMERID AS NVARCHAR(255)) 														AS ID_BUSINESS_KEY
		,CAST(NULL AS NCHAR(1)) 																AS CODE_STATUS
		,CAST(MODIFIEDDATE AS DATE)  															AS SK_DATE_MODIFIED
		,CAST(DBT_VALID_FROM AS DATE) 															AS SK_DATE_VALID_FROM
		,CAST(COALESCE(DBT_VALID_TO,'3000-12-31') AS DATE) 				    					AS SK_DATE_VALID_TO
		,CAST(TITLE AS NVARCHAR(255))															AS TEXT_TITLE
		,CAST(COMPANYNAME AS NVARCHAR(255))														AS TEXT_COMPANYNAME
		,CAST(FIRSTNAME AS NVARCHAR(255))														AS TEXT_FIRSTNAME
		,CAST(LASTNAME AS NVARCHAR(255))														AS TEXT_LASTNAME
		,CAST(CONCAT(FIRSTNAME,' ', LASTNAME) AS NVARCHAR(255)) 								AS TEXT_FULL_NAME
		,CAST(CONCAT(LPAD(TO_CHAR(CUSTOMERID), 6, '0'),' | '
					,FIRSTNAME,' '
					, LASTNAME) AS NVARCHAR(255))												AS TEXT_IDFULLNAME
		,CAST(SALESPERSON AS NVARCHAR(255))														AS TEXT_SALESPERSON
		,CAST(PHONE AS NVARCHAR(255))															AS TEXT_PHONE
		,CAST(EMAILADDRESS AS NVARCHAR(255))													AS TEXT_EMAILADDRESS
	FROM {{ ref('snap_aw_customer') }}
	
		UNION ALL
	
	-- creating row as a dimmension for null values in referrencing table
	SELECT 
		-1 																						AS SK_CUSTOMER
		,-1 																					AS SK_CUSTOMER_MASTER
		,'N/A' 																					AS ID_BUSINESS_KEY
		,'C'																					AS CODE_STATUS
		,'1900-01-01'				  															AS SK_DATE_MODIFIED
		,'1900-01-01'				 															AS SK_DATE_VALID_FROM
		,'3000-12-31'										 									AS SK_DATE_VALID_TO
		,'N/A'
		,'N/A'
		,'N/A'
		,'N/A'
		,'N/A'									
		,'N/A' 
		,'N/A'
		,'N/A'
		,'N/A'
	)
    SELECT 
        SK_CUSTOMER
        ,MIN(SK_CUSTOMER) OVER (PARTITION BY ID_BUSINESS_KEY 
							   ORDER BY SK_CUSTOMER) 											AS SK_CUSTOMER_MASTER
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
		,TEXT_TITLE
		,TEXT_COMPANYNAME
		,TEXT_FIRSTNAME
		,TEXT_LASTNAME
		,TEXT_FULL_NAME
		,TEXT_IDFULLNAME
		,TEXT_SALESPERSON
		,TEXT_PHONE
		,TEXT_EMAILADDRESS
    FROM SNAPSHOT_TABLE