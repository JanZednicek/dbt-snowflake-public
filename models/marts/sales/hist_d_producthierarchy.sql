/* 
-----------------------------------------------------------------------------
  About me: 
    - Powered by Jan Zednicek - https://janzednicek.cz
    - If you find any inconsistencies, please let me know - jsem@janzednicek.cz
    - Feel free to use this code without any limitations for learning (non-commercial use)
      If you find it helpful, consider giving credit by referencing my website
  
  About this table:
    - Table:   hist_d_product.sql
    - Source:  snap_aw_product.sql, snap_aw_productmodel.sql, snap_aw_productcategory.sql
    - Description: dimension table with surrogate keys, hist table contains complete history of 
				   records from primary source (all changes) based on SCD2. Curr table contains 
				   just current version (last known version of record)
				   We assign dummy -1 surrogate key (replacing NULL references)
				   and create date validity intervals from/to for each record
    - Log
        2024-02-11 - created
-----------------------------------------------------------------------------
*/
    
	WITH RECURSIVE CategoryHierarchy AS (
		SELECT 
			PRODUCTCATEGORYID,
			PARENTPRODUCTCATEGORYID,
			1 AS Level,
			CAST(PRODUCTCATEGORYID AS STRING) AS Level1,
			name AS Name1,
			NULL AS Level2,
			NULL AS Name2,
			DBT_VALID_FROM,
			DBT_VALID_TO
		FROM
			{{ ref('snap_aw_productcategory') }}
		WHERE
			PARENTPRODUCTCATEGORYID IS NULL -- Kořenové kategorie

		UNION ALL

		SELECT 
			t.PRODUCTCATEGORYID,
			t.PARENTPRODUCTCATEGORYID,
			ch.Level + 1 AS Level,
			ch.Level1,
			ch.Name1,
			CASE WHEN ch.Level = 1 THEN CAST(t.PRODUCTCATEGORYID AS STRING) ELSE ch.Level2 END AS Level2,
			CASE WHEN ch.Level = 1 THEN t.name ELSE ch.Name2 END AS Name2,
			ch.DBT_VALID_FROM,
			ch.DBT_VALID_TO
		FROM
			{{ ref('snap_aw_productcategory') }} t
		INNER JOIN
			CategoryHierarchy ch 
			ON t.PARENTPRODUCTCATEGORYID = ch.PRODUCTCATEGORYID
			AND t.DBT_VALID_FROM BETWEEN ch.DBT_VALID_FROM AND COALESCE(ch.DBT_VALID_TO, CURRENT_DATE()) -- Ujistěte se, že platnost podkategorie je v rámci platnosti rodiče
		WHERE
			ch.Level < 2
	), PRODUCTCATEGORY AS (

		SELECT 
			PRODUCTCATEGORYID,
			PARENTPRODUCTCATEGORYID,
			Name1 AS CategoryName_L1,
			COALESCE(Name2, Name1) AS CategoryName_L2, 
			DBT_VALID_FROM,
			DBT_VALID_TO
		FROM CategoryHierarchy
	
	), SNAPSHOT_TABLE AS (
	
	-- some standard stuff like casting, renaming etc.
	SELECT 
		CAST(ROW_NUMBER() OVER (ORDER BY PROD.PRODUCTID,prod._FIVETRAN_SYNCED) AS INT)			AS SK_PRODUCTHIERARCHY
		,CAST(NULL AS INT) 																		AS SK_PRODUCTHIERARCHY_MASTER
		,CAST(PROD.PRODUCTID AS NVARCHAR(255)) 													AS ID_BUSINESS_KEY
		,CAST(NULL AS NCHAR(1)) 																AS CODE_STATUS
		,CAST(PROD.MODIFIEDDATE AS DATE)  														AS SK_DATE_MODIFIED
		,CAST(PROD.DBT_VALID_FROM AS DATE) 														AS SK_DATE_VALID_FROM
		,CAST(COALESCE(PROD.DBT_VALID_TO,'3000-12-31') AS DATE)									AS SK_DATE_VALID_TO
		,CAST(PROD.DISCONTINUEDDATE AS DATE)		  											AS SK_DATE_DISCONTINUED
		,CAST(PROD.SELLENDDATE AS DATE)			  												AS SK_DATE_SELLEND 				    
		,CAST(PROD.LISTPRICE AS DECIMAL(18,2)) 													AS AMT_LISTPRICE
		,CAST(PROD.STANDARDCOST AS DECIMAL(18,2))			  									AS AMT_STANDARDCOST
		,CAST(PROD.PRODUCTNUMBER AS NVARCHAR(255))				  			    				AS CODE_PRODUCTNR
		,CAST(CONCAT(LPAD(TO_CHAR(PROD.PRODUCTID), 5, '0'), ' | ', PROD.NAME) AS NVARCHAR(255)) AS TEXT_PRODUCTCODENAME
		,CAST(PROD.NAME AS NVARCHAR(255))				    									AS TEXT_PRODUCTNAME
		,CAST(PRODMOD.NAME AS NVARCHAR(255))													AS TEXT_PRODUCTMODELNAME
		,CAST(PRODCAT.CategoryName_L1 AS NVARCHAR(255))											AS TEXT_PRODUCTCATEGORY
		,CAST(PRODCAT.CategoryName_L2 AS NVARCHAR(255))											AS TEXT_PRODUCTCATEGORY_PARENT
		,CAST(PROD.WEIGHT AS FLOAT) 															AS NUM_WEIGHT
	FROM {{ ref('snap_aw_product') }} PROD
		 LEFT JOIN {{ ref('snap_aw_productmodel') }} PRODMOD
			ON PROD.productmodelid = PRODMOD.productmodelid
			AND PROD.DBT_VALID_FROM BETWEEN PRODMOD.DBT_VALID_FROM AND COALESCE(PRODMOD.DBT_VALID_TO,'3000-12-31')
		 LEFT JOIN PRODUCTCATEGORY PRODCAT
		 	ON PROD.PRODUCTCATEGORYID = prodcat.PRODUCTCATEGORYID
			AND PROD.DBT_VALID_FROM BETWEEN prodcat.DBT_VALID_FROM AND COALESCE(prodcat.DBT_VALID_TO,'3000-12-31')


		UNION ALL
	
	-- creating row as a dimmension for null values in referrencing table
	SELECT 
		-1 																						AS SK_PRODUCTHIERARCHY
		,-1 																					AS SK_PRODUCTHIERARCHY_MASTER
		,'N/A' 																					AS ID_BUSINESS_KEY
		,'C'																					AS CODE_STATUS
		,'1900-01-01'				  															AS SK_DATE_MODIFIED
		,'1900-01-01'				 															AS SK_DATE_VALID_FROM
		,'3000-12-31'										 									AS SK_DATE_VALID_TO
		,'1900-01-01'	
		,'1900-01-01' 				    
		,0
		,0
		,'N/A'
		,'N/A'
		,'N/A'
		,'N/A'
		,'N/A'
		,'N/A'
		,0
	)
    SELECT 
        SK_PRODUCTHIERARCHY
        ,MIN(SK_PRODUCTHIERARCHY) OVER (PARTITION BY ID_BUSINESS_KEY 
							   ORDER BY SK_PRODUCTHIERARCHY) 									AS SK_PRODUCTHIERARCHY_MASTER
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
		,SK_DATE_DISCONTINUED
		,SK_DATE_SELLEND 				    
		,AMT_LISTPRICE
		,AMT_STANDARDCOST
		,CODE_PRODUCTNR
		,TEXT_PRODUCTCODENAME
		,TEXT_PRODUCTNAME
		,TEXT_PRODUCTMODELNAME
		,TEXT_PRODUCTCATEGORY
		,TEXT_PRODUCTCATEGORY_PARENT
		,NUM_WEIGHT
    FROM SNAPSHOT_TABLE