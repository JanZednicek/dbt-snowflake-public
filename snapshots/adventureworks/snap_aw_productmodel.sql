/* 
-----------------------------------------------------------------------------
  About me: 
    - Powered by Jan Zednicek - https://janzednicek.cz
    - If you find any inconsistencies, please let me know - jsem@janzednicek.cz
    - Feel free to use this code without any limitations for learning (non-commercial use) 
      If you find it helpful, consider giving credit by referencing my website
  
  About this table:
    - Table:    snap_aw_productmodel.sql
    - Schema:   staging | db based on your --target (profiles.yml)
    - Source:   adventureworks database (sqlservercentral.com) - table productmodel.sql | Data extracted using fivetran
    - Description: Regularly snapshotting this table to track changes and create new records in the staging schema
    - Log
        2024-02-11 - created
-----------------------------------------------------------------------------
*/

{% snapshot snap_aw_productmodel %}

    {{
        config(
          target_schema='STAGING',
          strategy='timestamp',
          unique_key='PRODUCTMODELID',
          updated_at='MODIFIEDDATE',
          invalidate_hard_deletes=True,
        )
    }}

    select * from {{ source('ADVENTUREWORKS', 'PRODUCTMODEL') }}

{% endsnapshot %}