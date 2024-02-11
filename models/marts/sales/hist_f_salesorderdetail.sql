/* 
-----------------------------------------------------------------------------
  About me: 
    - Powered by Jan Zednicek - https://janzednicek.cz
    - If you find any inconsistencies, please let me know - jsem@janzednicek.cz
    - Feel free to use this code without any limitations for learning (non-commercial use)
      If you find it helpful, consider giving credit by referencing my website
  
  About this table:
    - Table:   hist_f_salesorderheader.sql
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
		CAST(ROW_NUMBER() OVER (ORDER BY SALESORDERDETAILID,DRIVING._FIVETRAN_SYNCED) AS INT)	AS SK_SALESORDERDETAIL
		,CAST(NULL AS INT) 																		AS SK_SALESORDERDETAIL_MASTER
		,CAST(DRIVING.SALESORDERDETAILID AS NVARCHAR(255)) 										AS ID_BUSINESS_KEY
		,CAST(NULL AS NCHAR(1)) 																AS CODE_STATUS
		,CAST(DRIVING.MODIFIEDDATE AS DATE)  													AS SK_DATE_MODIFIED
		,CAST(DRIVING.DBT_VALID_FROM AS DATE) 													AS SK_DATE_VALID_FROM
		,CAST(COALESCE(DRIVING.DBT_VALID_TO,'3000-12-31') AS DATE) 				    			AS SK_DATE_VALID_TO
		,COALESCE(CAST(SALHEADER.SK_SALESORDERHEADER AS INT),-1)								AS SK_SALESORDERHEADER
		,COALESCE(CAST(SALHEADER.SK_SALESORDERHEADER_MASTER AS INT),-1)							AS SK_SALESORDERHEADER_MASTER
		,COALESCE(CAST(PRODUCT.SK_PRODUCTHIERARCHY AS INT),-1)									AS SK_PRODUCTHIERARCHY
		,COALESCE(CAST(PRODUCT.SK_PRODUCTHIERARCHY_MASTER AS INT),-1)							AS SK_PRODUCTHIERARCHY_MASTER
		,SALHEADER.SK_CUSTOMER
		,SALHEADER.SK_CUSTOMER_MASTER			
		,SALHEADER.SK_ADDRESS_SHIPPED
		,SALHEADER.SK_ADDRESS_SHIPPED_MASTER
		,SALHEADER.SK_ADDRESS_BILLED
		,SALHEADER.SK_ADDRESS_BILLED_MASTER		
		,CAST(DRIVING.ORDERQTY AS INT) 															AS AMT_ORDERQUANTITY
		,CAST(DRIVING.UNITPRICE AS DECIMAL(18,2)) 												AS AMT_UNITPRICE
		,CAST(DRIVING.UNITPRICEDISCOUNT AS DECIMAL(18,2)) 										AS AMT_UNITDISCOUNT
		,CAST(DRIVING.LINETOTAL AS DECIMAL(18,2)) 												AS AMT_LINETOTAL
		,CAST(PRODUCT.AMT_STANDARDCOST AS DECIMAL(18,2))*DRIVING.ORDERQTY   					AS AMT_STANDARTCOST	
		,CAST(DRIVING.LINETOTAL AS DECIMAL(18,2)) 
			-
		CAST(PRODUCT.AMT_STANDARDCOST AS DECIMAL(18,2))*DRIVING.ORDERQTY						AS AMT_MARGIN															
	FROM {{ ref('snap_aw_salesorderdetail') }} DRIVING
		 LEFT JOIN {{ ref('hist_d_salesorderheader') }} SALHEADER
		 	ON CAST(DRIVING.SALESORDERID AS NVARCHAR(255)) = SALHEADER.ID_BUSINESS_KEY
			AND DRIVING.DBT_VALID_FROM BETWEEN SALHEADER.SK_DATE_VALID_FROM_TYPE2 AND SALHEADER.SK_DATE_VALID_TO
		 LEFT JOIN {{ ref('hist_d_producthierarchy') }} PRODUCT
		 	ON CAST(DRIVING.PRODUCTID AS NVARCHAR(255)) = PRODUCT.ID_BUSINESS_KEY
			AND DRIVING.DBT_VALID_FROM BETWEEN PRODUCT.SK_DATE_VALID_FROM_TYPE2 AND PRODUCT.SK_DATE_VALID_TO

	/* 	NO NEED IN FACT TABLE

		UNION ALL
	
	-- creating row as a dimmension for null values in referrencing table
	SELECT 
		-1 																	AS SK_SALESORDERHEADER
		,-1 																AS SK_SALESORDERHEADER_BUSINESS_KEY
		,'N/A' 																AS ID_BUSINESS_KEY
		,'C'																AS CODE_STATUS
		,'1900-01-01'				  										AS SK_DATE_MODIFIED
		,'1900-01-01'				 										AS SK_DATE_VALID_FROM
		,'3000-12-31'										 				AS SK_DATE_VALID_TO
		,-1
		,-1
		,-1
		,-1
		,NULL
		,NULL
		,NULL
		,NULL */
	)
    SELECT 
        SK_SALESORDERDETAIL
        ,MIN(SK_SALESORDERDETAIL) OVER (PARTITION BY ID_BUSINESS_KEY 
							   ORDER BY SK_SALESORDERDETAIL) 									AS SK_SALESORDERDETAIL_MASTER
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
		,SK_SALESORDERHEADER
		,SK_SALESORDERHEADER_MASTER
		,SK_PRODUCTHIERARCHY
		,SK_PRODUCTHIERARCHY_MASTER
		,SK_CUSTOMER
		,SK_CUSTOMER_MASTER	
		,SK_ADDRESS_SHIPPED
		,SK_ADDRESS_SHIPPED_MASTER
		,SK_ADDRESS_BILLED
		,SK_ADDRESS_BILLED_MASTER	
		,AMT_ORDERQUANTITY
		,AMT_UNITPRICE
		,AMT_UNITDISCOUNT
		,AMT_LINETOTAL
		,AMT_STANDARTCOST	
		,AMT_MARGIN	
    FROM SNAPSHOT_TABLE