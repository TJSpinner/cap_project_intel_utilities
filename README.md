# cap_project_intel_utilities
Sample python codes, SQL queries, and Power BI dashboard for Capital Project Intelligence. All items revolve around analyzing 107 publicly-traded companies in the Utilities sector, extracting raw financial data, and transforming it into datasets that provide actionable insights to improve decision-making of stakeholders


Types of Data Extracted/Foundations for Analysis and Data Warehouse Organization
To start the ETL pipeline, there are 4 main python codes (with one more to be added) that extract raw data from two key sources: the EDGAR SEC site primarily, and a fallback stock analysis website. The 4 main types of financial company data are listed below:
  1) financial statement data, normalized and standardized for future analysis and export
  2) financial ratio data, utilized to compare companies to economic sector/industry benchmarks, as well as sequential and YoY percent change analyses
  3) stock price history data, gathered with the help of Polygons stock price API.  Uses concurrent threading to pull the stock price for 3500+ companies going back 5 years in roughly 10 minutes
  4) company screener data; aside from having interesting metrics itself, this screener table has crucial metadata that serve as the foundation for any potential star schema organization of the data warehouse, and provide multiple levels of financial analysis and comparative breakdowns. This screener table also provides the python codes to ability to filter each list of companies based on economic sector, which in this case is Utilities. So, instead of having to hardcode the company names in, the script dynamically creates the list of companies for each sector, filters for the desired sector, and adds all relevant metadata to the table. From there, it also provides the CIK number the code can utilize while extracting data from the EDGAR SEC database.


Step-by-step Function for Financial Statements
The financial statement python code is the most layered and complex, as unlike the financial ratio and stock price table, it utilzies multiple data sources, and also leverages FRED's API to automatically conduct a usd_to_company_currency forex conversion. The code begins by pulling the USD conversion rates for all major world currencies going back to 2008. That way, the financial data is an honest reflection of all companies financial health at that point in time - even foreign companies. Furthermore, all financial item values end up standardized to USD with the help of the automated forex conversion function.

After collecting all currency exchange rates  large concept and metric maps guide the code to gather all relevant financial statement items from the company info pages on EDGAR. In order to "clean" and standardize the data, these concept maps are fairly extensive and hardcoded into the python script. Seeing as these financial statement items remain the same, it is likely good practice to periodically update and expand these concept maps to the desired granularity of the financial analysis. At the end, any items that are synonymous, but have slightly different entries (e.g. - Revenue, Revenue as Reported), are normalized to match one another, preventing duplicates and confusion in the sort order.

The code comes with functions that sort the data specifically so users can either:
a) See the same item sequentially in order chronologically (e.g. - Revenue for Q1_2020, Q2_2020, etc..)
b) see every item from the financial statement in order for each year (e.g. - Revenue for Q1_2020, Cost of Revenue for Q1_2020, etc...)

Along with the order automatically sorted, the python code provides some key subtotals and calculations, such as gross margin, operating margin, EBIT, and a few more.  Some calcualted ratios include current ratio, quick ratio, Debt to Equity ratio, and more.  These ratios are placed next to the financial items utilized to calculate them, adding to the ease of utilizing this dataset for a wide range of financial analyses.

Aside from properly deriving and labeling every item in the financial statement tables properly, the strength of this code really shines in its "fallback" option. Should the code's search on the EDGAR database come back empty, or come back with no quarterly data (a common issue with foreign companies), the code has a "fallback" json it looks to fill in the gaps, or to fall back to completely. This process is done gracefully, with the code understanding exactly what to do for each of the three possible scenarios: an all-EDGAR extraction, a hybrid extraction, or a full fallback.  Regardless, all currencies get properly identified and standardized to USD, and all financial statement items get normalized, cleaned, and processed to the sql database ready to go.

The final piece of the code deals with creating materialized views and aggregate tables in sql. the code creates a sequential and YoY percent change materialized view, enabling change over time analysis "out of the box." The aggregate table simply unions together all individual company tables, and puts them in one place. Finally a materialized view is created for each statement type for each company, in case a highly granular analysis is required, comparing, for example, statements of equity from two different companies.


Financial Ratio and Stock Price History Codes
The financial ratio and stock price history codes have the same foundation of the financial statemennt codes, and utilize the same method to dynamically generate company lists based on economic sector, as well as the rich metadata tags. Similar to the financial statement code, these codes also create some percent change and monthly summary materialized views, as well as aggregate tables to make comparative analysis between companies, industries, sectors, market cap groups and more kinds of grouping easier. 





Part 2 of ETL Pipeline:
After all relevant data is collected, the etl code automatically extracts all data to a single data lake.  from here, all kinds of analyses are possible without any obstacle or technical issue. in this case, tables get a prefix added to the front of their name based on the database or schema they came from.  That way, they are easy to identify, while still being right next to each other in the new data lake database. Furthermore, all table names and column metadata are cleaned again to ensure queries involving join, union, and more are not held up by disparate data types.


Part 3 of the ETL Pipeline
Dynamic Queries can be helpful for financial analyses like this, providing the ability to instantly generate "industry" or "Sector" benchmark values for comparative analysis, or running averages and volatility that help better illustrate a company's changing performance over time. These can be provided upon request.


Part 4 of the ETL Pipeline
In order for stakeholders to understand the key takeaways from any financial analysis done with this data, power BI dashboards can easily be created from data imoprted by sql, and in conjunction with python library like matplotlib and seaborn.  Like the SQL queries, this Power BI dashboard will gladly be provided upon request.

