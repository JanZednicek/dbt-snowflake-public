/* 
-----------------------------------------------------------------------------
  About me: 
    - Powered by Jan Zednicek - https://janzednicek.cz
    - If you find any inconsistencies, please let me know - jsem@janzednicek.cz
    - Feel free to use this code without any limitations for learning (non-commercial use) 
      If you find it helpful, consider giving credit by referencing my website
  
  About this table:
    - Table:   curr_aw_address.sql
    - Schema:  staging | db based on your --target (profiles.yml)
    - Source:  staging schema - snap_aw_address.sql
    - Description: this table contains only current records from source table (no history/expired records)
    - Log
        2024-02-11 - created
-----------------------------------------------------------------------------
*/

select * 
from {{ source('ADVENTUREWORKS_SNAPSHOTS', 'SNAP_AW_ADDRESS') }} 
where dbt_valid_to is null
  