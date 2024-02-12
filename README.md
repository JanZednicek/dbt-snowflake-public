<h1>Sample Data warehouse | Datamart (Data Engineering project)</h1>
<ul>
 	<li>This repository contains dbt project only</li>
 	<li>Author: <a href="https://janzednicek.cz"> Jan Zednicek</a> - If you find any inconsistencies, please let me know - <a href="mailto:jsem@janzednicek.cz">jsem@janzednicek.cz</a></li>
 	<li>Feel free to use this code without any limitations for learning (non-commercial) use.</li>
</ul>
<h2><strong>1 Project description</strong></h2>
<p>The goal of this project is to create the structure of a data warehouse or a data mart, incorporating staging and increment data layers. We will utilize sample sales data from the AdventureWorks database on SQL Server. Our database structures will be implemented in Snowflake. Extraction from the source system will be conducted using Fivetran, and transformations will be carried out using dbt.</p>

<h3>We aim to achieve:</h3>
<ul>
  <li>Extract data using fivetran, snapshoting/transformations using dbt</li>
  <li>Implementation of Slowly Changing Dimension Type 2.</li>
  <li>Create data warehouse model based on star schema</li>
  <li>Create Orchestration engine using dbt references</li>
  <li>Using clear and consistent naming conventions.</li>
  <li>Development (dev) and (prod) profiles in dbt for testing purposes.</li>
</ul>

<h3>Recap:</h3>
<ul dir="auto">
 	<li><strong>Source:</strong> <a href="https://www.sqlservercentral.com/articles/connecting-to-adventureworks-on-azure">SQL Server - AdventureWorks</a></li>
 	<li><strong>Extract/Load:</strong> <a href="https://fivetran.com">Fivetran</a></li>
 	<li><strong>Transformations:</strong> <a href="https://www.getdbt.com">dbt (data build tool) – this repository</a></li>
 	<li><strong>Destination:</strong> <a href="https://www.snowflake.com/en/">Snowflake</a></li>
</ul>
<h2>2 Source data (Fictional CRM - AdventureWorks)</h2>
<p>Below you can find Source data ER diagram. Our data consist of 7 tables. These tables are not heavy (couple of thousands of rows)</p>
<ul>
 	<li><strong>Address</strong> – list of addresses (shipment/billing)</li>
 	<li><strong>Customer</strong>  – list of customers</li>
 	<li><strong>Product</strong>  – list of products together with prices and standard costs</li>
 	<li><strong>Productmodel</strong>  – a list of product models (each product is associated with a specific model).</li>
 	<li><strong>Productcategory</strong>  – list of product categories (self referrenced parent-child structure). Each product is associated with a specific category tree.</li>
 	<li><strong>Salesorderheader</strong>  – sales orders ordered by our customers</li>
 	<li><strong>Salesorderdetail</strong>  – sales order line items (each order contains items -some products in it)</li>
</ul>
<img src="https://github.com/JanZednicek/dbt-snowflake-public/assets/39887295/a00a8a4f-a0c7-4a4c-a321-91d59a1276b7" alt="Source Data ER Diagram">

<h2>3 Destination data (Snowflake) – Data warehouse structure</h2>
We will transform our data using dbt to the star schema below.

<img src="https://github.com/JanZednicek/dbt-snowflake-public/assets/39887295/d659bfe0-6335-475b-994a-2399e055ea6a" alt="Source Data ER Diagram">

<h2>4 Steplist</h2>
<ol>
 	<li>Create <a href="https://fivetran.com/" target="_blank">Fivetran</a> account (trial and 500 000 rows for free)</li>
 	<li>Create <a href="https://www.snowflake.com/" target="_blank">Snowflake</a> account (30 days trial)
<ul>
 	<li>Copy <a href="https://www.sqlservercentral.com/articles/connecting-to-adventureworks-on-azure" target="_blank">AdventureWorks</a> data using Fivetran OR manually to Snowflake database (give it a name “ADVENTUREWORKS”, schema “SQL_SERVER_SALESLT”)</li>
 	<li>Create database “DBT_DEV” db environment
<ul>
 	<li>create schema “STAGING”</li>
 	<li>create schema “SALES_MART”,</li>
 	<li>create user DBT_DEV_USER and give him a ACCOUNTADMIN role</li>
</ul>
</li>
 	<li>Create database for “DBT_PROD” db environment
<ul>
 	<li>create schema “STAGING”</li>
 	<li>create schema “SALES_MART”</li>
 	<li>create user DBT_PROD_USER and give him a ACCOUNTADMIN role</li>
</ul>
</li>
 	<li>Create warehouse for query processing</li>
</ul>
</li>
 	<li>Clone this repository to you local folder</li>
 	<li>Install <a href="https://www.python.org/" target="_blank">python</a></li>
 	<li>Open command line/terminal and <a href="https://docs.python.org/3/library/venv.html" target="_blank">create new environment</a></li>
 	<li>Activate new environment and install dbt with snowflake adapter using “<strong>pip install dbt-snowflake</strong>”</li>
 	<li>Open your dbt location (step 3) and open file named <strong>profiles.yml </strong>and fill your snowflake credentials</li>
  <br><img width="1017" alt="image" src="https://github.com/JanZednicek/dbt-snowflake-public/assets/39887295/869aeca0-13e8-4bb3-9390-ad4506594a6f">
 	<li>In command line (terminal), go to your dbt location and run “<strong>dbt debug --target dev</strong>” and then “<strong>dbt debug --target prod</strong>”. It tests you connection.</li>
 	<li>Run “<strong>dbt run –target dev</strong>” if successful you have you data in star schema model</li>
 	<li>Create some cool reports</li>
</ol>

<h2><u> 5 Naming conventions - to keep all organized</u>:</h2>

<strong>A) Tables</strong>
<ul>
 	<li>Hist_Table – contain all historical versions – slowly changing dimensions 2</li>
 	<li>Curr_Table – contain only last known current version</li>
 	<li>D_ - Dimension table</li>
 	<li>F_ - Fact table</li>
</ul>
<p><strong>B) Columns</strong></p>
<ul>
 	<li>SK_TABLE – Surrogate key – primary key</li>
 	<li>SK_TABLE_MASTER – Surrogate key – business key (consistent over all historical versions of the same business key)</li>
 	<li>ID_BUSINESS_KEY – original business key</li>
 	<li>CODE_STATUS (C,D,E) – Current (C), Expired (E), Deleted (D)</li>
 	<li>TEXT_COLUMN – nvarchar type of information</li>
 	<li>NUM_COLUMN – numerical value</li>
 	<li>AMT_COLUMN – amount (money, quantity)</li>
</ul>
<p><strong>C) dbt file organization</strong></p>
<strong>1) snapshot</strong>
<ul>
 	<li>source_system1 (folder)
<ul>
 	<li>source_system1_rawtable1</li>
 	<li>source_system1_rawtable2</li>
</ul>
</li>
 	<li>source_system2 (folder)</li>
 	<li>source_system3 (folder)</li>
</ul>
<strong>2) model</strong>
<ul>
 	<li>marts (semantic layer)
<ul>
 	<li>sales
<ul>
 	<li>curr_d_table1</li>
 	<li>hist_d_table1</li>
</ul>
</li>
 	<li>marketing</li>
 	<li>finance</li>
</ul>
</li>
 	<li>staging (raw data)
<ul>
 	<li>source_system1 (folder)
<ul>
 	<li>curr_source_system_rawtable1</li>
 	<li>curr_source_system_rawtable2</li>
 	<li>snap_source_system_rawtable1</li>
 	<li>snap_source_system_rawtable2</li>
</ul>
</li>
 	<li>source_system2 (folder)</li>
 	<li>source_system3 (folder)</li>
</ul>
</li>
</ul>
<img width="350" alt="dbt_file_structure" src="https://github.com/JanZednicek/dbt-snowflake-public/assets/39887295/342f2e35-47b5-43ae-8e56-51c34e750e0a">

<h2>6 Code examples</h2>
<ol>
<ul>
   <li> <a href="https://github.com/JanZednicek/dbt-snowflake-public/blob/main/models/marts/sales/hist_d_producthierarchy.sql" target="_blank">Semantic layer dimension table product - hist_d_producthierarchy</a></li>
 </li>
  <li> <a href="https://github.com/JanZednicek/dbt-snowflake-public/blob/main/snapshots/adventureworks/snap_aw_product.sql" target="_blank">Snapshot table product - snap_aw_product</a></li>
  <li> <a href="https://github.com/JanZednicek/dbt-snowflake-public/blob/main/models/marts/sales/d_date.sql" target="_blank">Semantic layer dimension table date - D_Date</a></li>
</ul>
</ol>
<h2>7 Power BI report</h2>
<p>Comming soon</p>

