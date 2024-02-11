<h1>Sample Data warehouse | Datamart (Data Engineering project)</h1>
<ul>
 	<li>This repository contains dbt project only</li>
 	<li>Author: Jan Zednicek - <a href="https://janzednicek.cz/">https://janzednicek.cz</a>  If you find any inconsistencies, please let me know - <a href="mailto:jsem@janzednicek.cz">jsem@janzednicek.cz</a></li>
 	<li>Feel free to use this code without any limitations for learning (non-commercial use).</li>
</ul>
<h2><strong>1 Project description</strong></h2>
<p>The goal of this project is to create the structure of a data warehouse or a data mart, incorporating staging and increment data layers. We will utilize sample sales data from the AdventureWorks database on SQL Server. Our database structures will be implemented in Snowflake. Extraction from the source system will be conducted using Fivetran, and transformations will be carried out using dbt.</p>

<h3>We aim to achieve:</h3>
<ul>
  <li>A visually appealing data warehouse model, ideally adhering to the star schema, based on the extracted data.</li>
  <li>Clear and consistent naming conventions.</li>
  <li>Implementation of Slowly Changing Dimension Type 2.</li>
  <li>Development (dev) and (prod) profiles in dbt for testing purposes.</li>
</ul>

<h3>Recap:</h3>
<ul>
  <li><strong>Source:</strong> SQL Server - AdventureWorks</li>
  <li><strong>Extract/Load:</strong> Fivetran</li>
  <li><strong>Transformations:</strong> dbt (data build tool) – this repository</li>
  <li><strong>Destination:</strong> Snowflake</li>
</ul>
<h2>2 Source data (Fictional CRM - AdventureWorks)</h2>
<p>Below you can find Source data ER diagram. Our data consist of 7 tables. These tables are not heavy (couple of thousands of rows)</p>
<ul>
 	<li>Address – list of addresses (shipment/billing)</li>
 	<li>Customer – list of customers</li>
 	<li>Product – list of products together with prices and standard costs</li>
 	<li>Productmodel – a list of product models (each product is associated with a specific model).</li>
 	<li>Productcategory – list of product categories (self referrenced parent-child structure). Each product is associated with a specific category tree.</li>
 	<li>Salesorderheader – sales orders ordered by our customers</li>
 	<li>Salesorderdetail – sales order line items (each order contains items -some products in it)</li>
</ul>
<img src="https://github.com/JanZednicek/dbt-snowflake-public/assets/39887295/a00a8a4f-a0c7-4a4c-a321-91d59a1276b7" alt="Source Data ER Diagram">

<h2>3 Destination data (Snowflake) – Data warehouse structure</h2>
We will transform our data using dbt to the star schema below.

<img src="https://github.com/JanZednicek/dbt-snowflake-public/assets/39887295/d659bfe0-6335-475b-994a-2399e055ea6a" alt="Source Data ER Diagram">
<strong><u>Naming conventions</u>:</strong>

<strong>A) Tables</strong>
<ul>
 	<li>Hist_Table – contain all historical versions – slowly changing dimensions 2</li>
 	<li>Curr_Table – contain only last known current version</li>
 	<li>D_ - Dimension table</li>
 	<li>F_ - Fact table</li>
</ul>
<strong>B) Columns</strong>
<ul>
 	<li>SK_TABLE – Surrogate key – primary key</li>
 	<li>SK_TABLE_MASTER – Surrogate key – business key (consistent over all historical versions of the same business key)</li>
 	<li>ID_BUSINESS_KEY – original business key</li>
 	<li>CODE_STATUS (C,D,E) – Current (C), Expired (E), Deleted (D)</li>
 	<li>TEXT_COLUMN – nvarchar type of information</li>
 	<li>NUM_COLUMN – numerical value</li>
 	<li>AMT_COLUMN – amount (money, quantity)</li>
</ul>
<h2>4 Steplist</h2>
<ol>
 	<li>Create <a href="https://fivetran.com/">Fivetran</a> account (trial and 500 000 rows for free)</li>
 	<li>Create <a href="https://www.snowflake.com/">Snowflake</a> account (30 days trial)
<ul>
 	<li>Copy <a href="https://www.sqlservercentral.com/articles/connecting-to-adventureworks-on-azure">AdventureWorks</a> data using Fivetran OR manually to Snowflake database (give it a name “ADVENTUREWORKS”, schema “SQL_SERVER_SALESLT”)</li>
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
 	<li>Install <a href="https://www.python.org/">python</a></li>
 	<li>Open command line/terminal and <a href="https://docs.python.org/3/library/venv.html">create new environment</a></li>
 	<li>Activate new environment and install dbt with snowflake adapter using “<strong>pip install dbt-snowflake</strong>”</li>
 	<li>Open you dbt location (step 4) and open file named <strong>yml </strong>and fill your snowflake credentials</li>
 	<li>In command line (terminal), go to your dbt location and run “<strong>dbt debug --target dev</strong>” and then “<strong>dbt debug --target prod</strong>”. It tests you connection.</li>
 	<li>Run “<strong>dbt run –target dev</strong>” if successful you have you data in star schema model</li>
 	<li>Create some cool reports</li>
</ol>
