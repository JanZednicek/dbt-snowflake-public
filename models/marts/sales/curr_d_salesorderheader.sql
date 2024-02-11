   /* 
-----------------------------------------------------------------------------
  About me: 
    - Powered by Jan Zednicek - https://janzednicek.cz
    - If you find any inconsistencies, please let me know - jsem@janzednicek.cz
    - Feel free to use this code without any limitations for learning (non-commercial use)
      If you find it helpful, consider giving credit by referencing my website
  
  About this table:
    - Table:   curr_d_salesorderheader.sql
    - Source:  hist_d_salesorderheader.sql
    - Description: dimension table with surrogate keys. Curr table contains just current version 
                   (last known version of record) - you can find the main transformation logic 
                   in source table hist_d_salesorderheader.sql
    - Log
        2024-02-11 - created
-----------------------------------------------------------------------------
*/

	WITH CURRENT_TABLE AS (
	SELECT 
        SK_SALESORDERHEADER_MASTER
        ,ID_BUSINESS_KEY
		,CODE_STATUS 
        ,SK_DATE_MODIFIED
        ,SK_DATE_VALID_FROM_TYPE2
        ,SK_DATE_VALID_FROM
        ,SK_DATE_VALID_TO
		,SK_DATE_SHIPDATE
		,SK_DATE_ORDERDATE
		,SK_DATE_DUEDATE
		,SK_CUSTOMER_MASTER
		,SK_ADDRESS_SHIPPED_MASTER
		,SK_ADDRESS_BILLED_MASTER
		,CODE_ACCOUNTNUMBER
		,CODE_PURCHASENUMBER
		,CODE_SALESORDERNUMBER
		,CODE_SHIPMETHOD
    FROM {{ ref('hist_d_salesorderheader') }}
	WHERE CODE_STATUS = 'C'
	)
	SELECT 
	*
	FROM CURRENT_TABLE