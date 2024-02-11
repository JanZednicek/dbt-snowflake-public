 /* 
-----------------------------------------------------------------------------
  About me: 
    - Powered by Jan Zednicek - https://janzednicek.cz
    - If you find any inconsistencies, please let me know - jsem@janzednicek.cz
    - Feel free to use this code without any limitations for learning (non-commercial use)
      If you find it helpful, consider giving credit by referencing my website
  
  About this table:
    - Table:   curr_d_customer.sql
    - Source:  hist_d_customer.sql
    - Description: dimension table with surrogate keys. Curr table contains just current version 
                   (last known version of record) - you can find the main transformation logic 
                   in source table hist_d_customer.sql
    - Log
        2024-02-11 - created
-----------------------------------------------------------------------------
*/

	WITH CURRENT_TABLE AS (
	SELECT 
        SK_CUSTOMER
        ,SK_CUSTOMER_MASTER
        ,ID_BUSINESS_KEY
		,CODE_STATUS 
        ,SK_DATE_MODIFIED
        ,SK_DATE_VALID_FROM_TYPE2
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
    FROM {{ ref('hist_d_customer') }}
	WHERE CODE_STATUS = 'C'
	)
	SELECT 
	*
	FROM CURRENT_TABLE