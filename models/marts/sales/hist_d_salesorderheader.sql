/* 
-----------------------------------------------------------------------------
  About me: 
    - Powered by Jan Zednicek - https://janzednicek.cz
    - If you find any inconsistencies, please let me know - jsem@janzednicek.cz
    - Feel free to use this code without any limitations for learning (non-commercial use)
      If you find it helpful, consider giving credit by referencing my website
  
  About this table:
    - Table:   hist_d_salesorderheader.sql
    - Source:  snap_aw_salesorderheader.sql
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
		CAST(ROW_NUMBER() OVER (ORDER BY SALESORDERID,DRIVING._FIVETRAN_SYNCED) AS INT)			AS SK_SALESORDERHEADER
		,CAST(NULL AS INT) 																		AS SK_SALESORDERHEADER_MASTER
		,CAST(DRIVING.SALESORDERID AS NVARCHAR(255)) 											AS ID_BUSINESS_KEY
		,CAST(NULL AS NCHAR(1)) 																AS CODE_STATUS
		,CAST(DRIVING.MODIFIEDDATE AS DATE)  													AS SK_DATE_MODIFIED
		,CAST(DRIVING.DBT_VALID_FROM AS DATE) 													AS SK_DATE_VALID_FROM
		,CAST(COALESCE(DRIVING.DBT_VALID_TO,'3000-12-31') AS DATE) 				    			AS SK_DATE_VALID_TO
		,CAST(DRIVING.SHIPDATE AS DATE)  														AS SK_DATE_SHIPDATE
		,CAST(DRIVING.ORDERDATE AS DATE)  														AS SK_DATE_ORDERDATE
		,CAST(DRIVING.DUEDATE AS DATE)  														AS SK_DATE_DUEDATE
		,COALESCE(CAST(CUST.SK_CUSTOMER AS INT),-1)      										AS SK_CUSTOMER
		,COALESCE(CAST(CUST.SK_CUSTOMER_MASTER AS INT),-1)         								AS SK_CUSTOMER_MASTER
		,COALESCE(CAST(ADDRESS_SHIPPED.SK_ADDRESS AS INT),-1) 									AS SK_ADDRESS_SHIPPED
		,COALESCE(CAST(ADDRESS_SHIPPED.SK_ADDRESS_MASTER AS INT),-1) 							AS SK_ADDRESS_SHIPPED_MASTER
		,COALESCE(CAST(ADDRESS_BILLED.SK_ADDRESS AS INT),-1) 									AS SK_ADDRESS_BILLED
		,COALESCE(CAST(ADDRESS_BILLED.SK_ADDRESS_MASTER AS INT),-1) 							AS SK_ADDRESS_BILLED_MASTER
		,CAST(DRIVING.ACCOUNTNUMBER AS NVARCHAR(255)) 											AS CODE_ACCOUNTNUMBER
		,CAST(DRIVING.PURCHASEORDERNUMBER AS NVARCHAR(255)) 									AS CODE_PURCHASENUMBER
		,CAST(DRIVING.SALESORDERNUMBER AS NVARCHAR(255)) 										AS CODE_SALESORDERNUMBER
		,CAST(DRIVING.SHIPMETHOD AS NVARCHAR(255)) 												AS CODE_SHIPMETHOD
	FROM {{ ref('snap_aw_salesorderheader') }} DRIVING
		 LEFT JOIN {{ ref('hist_d_customer') }} CUST
		 	ON CAST(DRIVING.CUSTOMERID AS NVARCHAR(255)) = CUST.ID_BUSINESS_KEY
			AND DRIVING.DBT_VALID_FROM BETWEEN CUST.SK_DATE_VALID_FROM_TYPE2 AND CUST.SK_DATE_VALID_TO
		 LEFT JOIN {{ ref('hist_d_address') }} ADDRESS_SHIPPED
		 	ON CAST(DRIVING.SHIPTOADDRESSID AS NVARCHAR(255)) = ADDRESS_SHIPPED.ID_BUSINESS_KEY
			AND DRIVING.DBT_VALID_FROM BETWEEN ADDRESS_SHIPPED.SK_DATE_VALID_FROM_TYPE2 AND ADDRESS_SHIPPED.SK_DATE_VALID_TO
		 LEFT JOIN {{ ref('hist_d_address') }} ADDRESS_BILLED
		 	ON CAST(DRIVING.BILLTOADDRESSID AS NVARCHAR(255)) = ADDRESS_BILLED.ID_BUSINESS_KEY
			AND DRIVING.DBT_VALID_FROM BETWEEN ADDRESS_BILLED.SK_DATE_VALID_FROM_TYPE2 AND ADDRESS_BILLED.SK_DATE_VALID_TO
	
		UNION ALL
	
	-- creating row as a dimmension for null values in referrencing table
 	SELECT 
		-1 																						AS SK_SALESORDERHEADER
		,-1 																					AS SK_SALESORDERHEADER_MASTER
		,'N/A' 																					AS ID_BUSINESS_KEY
		,'C'																					AS CODE_STATUS
		,'1900-01-01'				  															AS SK_DATE_MODIFIED
		,'1900-01-01'				 															AS SK_DATE_VALID_FROM
		,'3000-12-31'										 									AS SK_DATE_VALID_TO
		,'1900-01-01'
		,'1900-01-01'
		,'1900-01-01'
		,-1
		,-1
		,-1
		,-1
		,-1
		,-1
		,'N/A'
		,'N/A'
		,'N/A'
		,'N/A' 
	)
    SELECT 
        SK_SALESORDERHEADER
        ,MIN(SK_SALESORDERHEADER) OVER (PARTITION BY ID_BUSINESS_KEY 
							   ORDER BY SK_SALESORDERHEADER) 									AS SK_SALESORDERHEADER_MASTER
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
		,SK_DATE_SHIPDATE
		,SK_DATE_ORDERDATE
		,SK_DATE_DUEDATE
		,SK_CUSTOMER
		,SK_CUSTOMER_MASTER
		,SK_ADDRESS_SHIPPED
		,SK_ADDRESS_SHIPPED_MASTER
		,SK_ADDRESS_BILLED
		,SK_ADDRESS_BILLED_MASTER
		,CODE_ACCOUNTNUMBER
		,CODE_PURCHASENUMBER
		,CODE_SALESORDERNUMBER
		,CODE_SHIPMETHOD
    FROM SNAPSHOT_TABLE